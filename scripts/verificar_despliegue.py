"""Verifica un despliegue completo del ecosistema, de punta a punta, como lo usaría el front.

    python scripts/verificar_despliegue.py --gateway https://dda2-g8-orquestador.onrender.com \
        --monitor https://dda2-g8-monitor.onrender.com

Pasos (cada uno informa OK o FALLA; termina con código 1 si alguno falla):

1. Despierta a todos los servicios con el monitor (`POST /monitor/despertar`), si se pasa `--monitor`.
2. `/health` del orquestador: base y Kafka arriba.
3. Login del operador en auth, por el gateway. Por defecto usa ADMIN_EMAIL y ADMIN_PASSWORD de despliegue.local/.
4. Estado de cada servicio según el monitor, si se pasa `--monitor`.
5. Unidades disponibles en vehiculos, con patente.
6. Un lead nuevo (preformulario público) hasta que su flujo REGISTRO_LEAD quede COMPLETADO: pasa por leads,
   clientes, calificacion e ia-resumen. Con `--sin-llm` alcanza con que quede calificado.
7. Una consulta de multas (unidad con patente AAA111, o la primera con patente) hasta COMPLETADO: pasa por el
   scraper y vehiculos.

En Render free los servicios dormidos tardan hasta ~1 min en despertar: por eso las esperas son largas.
"""

from __future__ import annotations

import argparse
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import httpx

RAIZ = Path(__file__).resolve().parent.parent
PATENTE_DE_PRUEBA = "AAA111"
TERMINALES = ("COMPLETADO", "FALLIDO", "EXPIRADO")
# Estados de REGISTRO_LEAD que ya pasaron la calificación.
CALIFICADO_O_MAS = ("CALIFICADO", "RESUMEN_PENDIENTE", "COMPLETADO")


@dataclass
class Resultado:
    paso: str
    ok: bool
    detalle: str


@dataclass
class Verificador:
    http: httpx.Client
    gateway: str
    monitor: Optional[str] = None
    email: Optional[str] = None
    password: Optional[str] = None
    sin_llm: bool = False
    espera_maxima: float = 600.0
    intervalo: float = 5.0
    dormir: Callable[[float], None] = time.sleep
    reloj: Callable[[], float] = time.monotonic
    token: Optional[str] = None
    resultados: list[Resultado] = field(default_factory=list)

    # ------------------------------------------------------------ utilidades

    def _api(self, ruta: str) -> str:
        return f"{self.gateway.rstrip('/')}{ruta}"

    def _cabeceras(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    def _anotar(self, paso: str, ok: bool, detalle: str) -> bool:
        self.resultados.append(Resultado(paso, ok, detalle))
        print(f"[{'OK' if ok else 'FALLA'}] {paso}: {detalle}", flush=True)
        return ok

    def _esperar_flujo(self, clave: str, tipo: str, listo: Callable[[str], bool]) -> Optional[dict[str, Any]]:
        """El flujo de esa clave hasta que `listo(estado)` o un estado terminal. None si se agota la espera."""
        fin = self.reloj() + self.espera_maxima
        while True:
            respuesta = self.http.get(self._api("/api/orquestador/flujos"), params={"leadId": clave, "tipo": tipo},
                                      headers=self._cabeceras())
            if respuesta.status_code == 200 and respuesta.json():
                flujo = respuesta.json()[0]
                if listo(flujo["estado"]) or flujo["estado"] in TERMINALES:
                    return flujo
            if self.reloj() >= fin:
                return None
            self.dormir(self.intervalo)

    @staticmethod
    def _motivo(respuesta: httpx.Response) -> str:
        """El detalle de un error: los campos rechazados si los trae, o el principio del cuerpo."""
        try:
            cuerpo = respuesta.json()
        except ValueError:
            return respuesta.text[:200]
        errores = cuerpo.get("errores") or cuerpo.get("detail")
        if isinstance(errores, list):
            return "; ".join(f"{e.get('campo') or e.get('loc')}: {e.get('mensaje') or e.get('msg')}" for e in errores)[:300]
        return str(cuerpo.get("detail") or cuerpo)[:300]

    @staticmethod
    def _pasos(flujo: dict[str, Any]) -> str:
        return ", ".join(f"{p['paso']}={p['estado']}" for p in flujo.get("pasos", []))

    # ------------------------------------------------------------ pasos

    def despertar(self) -> bool:
        if not self.monitor:
            return True
        respuesta = self.http.post(f"{self.monitor.rstrip('/')}/monitor/despertar")
        return self._anotar("despertar servicios", respuesta.status_code < 300, f"monitor respondió {respuesta.status_code}")

    def salud_del_gateway(self) -> bool:
        respuesta = self.http.get(self._api("/health"))
        if respuesta.status_code != 200:
            return self._anotar("salud del orquestador", False, f"/health respondió {respuesta.status_code}")
        componentes = respuesta.json().get("componentes", {})
        caidos = [n for n, c in componentes.items() if c.get("status") != "UP"]
        return self._anotar("salud del orquestador", not caidos,
                            "base y Kafka arriba" if not caidos else f"caídos: {', '.join(caidos)}")

    def login(self) -> bool:
        if not self.email:
            return self._anotar("login del operador", True, "omitido (sin credenciales)")
        respuesta = self.http.post(self._api("/api/auth/login"), json={"email": self.email, "password": self.password})
        if respuesta.status_code != 200:
            return self._anotar("login del operador", False, f"auth respondió {respuesta.status_code}")
        self.token = respuesta.json()["access_token"]
        return self._anotar("login del operador", True, f"token para {self.email}")

    def estado_de_los_servicios(self) -> bool:
        if not self.monitor:
            return True
        respuesta = self.http.get(f"{self.monitor.rstrip('/')}/monitor/estado", params={"forzar": "true"})
        if respuesta.status_code != 200:
            return self._anotar("estado según el monitor", False, f"monitor respondió {respuesta.status_code}")
        estados = {s.get("servicio"): s.get("estado") for s in respuesta.json()}
        # DEGRADADO = responde pero lento (un servicio recién despertado): no es una falla.
        caidos = sorted(n for n, e in estados.items() if e not in ("UP", "DEGRADADO"))
        lentos = sorted(n for n, e in estados.items() if e == "DEGRADADO")
        detalle = f"{len(estados)} servicios respondiendo" + (f" (lentos: {', '.join(lentos)})" if lentos else "")
        return self._anotar("estado según el monitor", not caidos, detalle if not caidos else f"caídos: {', '.join(caidos)}")

    def unidad_con_patente(self) -> Optional[dict[str, Any]]:
        respuesta = self.http.get(self._api("/api/vehiculos"), params={"disponible": "true"})
        if respuesta.status_code != 200:
            self._anotar("unidades disponibles", False, f"vehiculos respondió {respuesta.status_code}")
            return None
        unidades = respuesta.json()
        con_patente = [u for u in unidades if u.get("patente")]
        elegida = next((u for u in con_patente if u["patente"] == PATENTE_DE_PRUEBA), con_patente[0] if con_patente else None)
        self._anotar("unidades disponibles", elegida is not None,
                     f"{len(unidades)} disponibles, {len(con_patente)} con patente"
                     + (f"; se usa {elegida['patente']}" if elegida else ""))
        return elegida

    def flujo_de_un_lead(self, vehiculo_id: str) -> bool:
        sufijo = uuid.uuid4().hex[:8]
        # clientes solo acepta letras en nombre y apellido: el sufijo va en letras (a-p) para que cada corrida sea otro cliente.
        apellido = "Despliegue " + "".join(chr(ord("a") + int(c, 16)) for c in sufijo).capitalize()
        preformulario = {
            "cliente": {"nombre": "Prueba", "apellido": apellido, "email": f"prueba+{sufijo}@dda2-crm.com",
                        "telefono": "1100000000"},
            "vehiculoId": vehiculo_id,
            "anticipo": 6500000,
            "permuta": {"tienePermuta": False},
            "ubicacion": {"provincia": "Buenos Aires", "localidad": "CABA"},
            "solicitaFinanciacion": False,
        }
        respuesta = self.http.post(self._api("/api/leads"), json=preformulario)
        if respuesta.status_code not in (200, 201, 202):
            return self._anotar("lead de punta a punta", False, f"leads respondió {respuesta.status_code}: {self._motivo(respuesta)}")
        lead_id = respuesta.json().get("lead_id") or respuesta.json().get("leadId")
        meta = CALIFICADO_O_MAS if self.sin_llm else ("COMPLETADO",)
        flujo = self._esperar_flujo(lead_id, "REGISTRO_LEAD", lambda estado: estado in meta)
        if flujo is None:
            return self._anotar("lead de punta a punta", False, f"lead {lead_id}: sin flujo terminado a tiempo")
        return self._anotar("lead de punta a punta", flujo["estado"] in meta,
                            f"lead {lead_id} en {flujo['estado']} ({self._pasos(flujo)})")

    def flujo_de_multas(self, unidad: dict[str, Any]) -> bool:
        respuesta = self.http.post(self._api(f"/api/orquestador/multas/{unidad['id']}"), headers=self._cabeceras())
        if respuesta.status_code not in (202, 409):  # 409: ya hay una en curso (el barrido o el alta la abrió)
            return self._anotar("multas de punta a punta", False, f"el orquestador respondió {respuesta.status_code}")
        flujo = self._esperar_flujo(unidad["id"], "CONSULTA_MULTAS_VEHICULO", lambda estado: estado == "COMPLETADO")
        if flujo is None:
            return self._anotar("multas de punta a punta", False, f"{unidad['patente']}: sin terminar a tiempo")
        return self._anotar("multas de punta a punta", flujo["estado"] == "COMPLETADO",
                            f"{unidad['patente']} en {flujo['estado']} ({self._pasos(flujo)})"
                            + (f": {flujo['ultimoError']}" if flujo.get("ultimoError") else ""))

    def correr(self) -> bool:
        self.despertar()
        if not self.salud_del_gateway():
            return False  # sin gateway no hay nada más que probar
        if not self.login():
            return False
        self.estado_de_los_servicios()
        unidad = self.unidad_con_patente()
        if unidad is not None:
            self.flujo_de_un_lead(unidad["id"])
            self.flujo_de_multas(unidad)
        return all(r.ok for r in self.resultados)


def _credenciales_locales() -> tuple[Optional[str], Optional[str]]:
    """ADMIN_EMAIL y ADMIN_PASSWORD de despliegue.local/, si existen (los arma preparar_despliegue.py)."""
    valores: dict[str, str] = {}
    for nombre in ("proveedores.env", "secretos.env"):
        ruta = RAIZ / "despliegue.local" / nombre
        if ruta.exists():
            for linea in ruta.read_text(encoding="utf-8").splitlines():
                if "=" in linea and not linea.lstrip().startswith("#"):
                    clave, _, valor = linea.partition("=")
                    valores[clave.strip()] = valor.strip()
    return valores.get("ADMIN_EMAIL"), valores.get("ADMIN_PASSWORD")


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Verifica el ecosistema desplegado de punta a punta.")
    parser.add_argument("--gateway", required=True, help="URL del orquestador, ej. https://dda2-g8-orquestador.onrender.com")
    parser.add_argument("--monitor", help="URL del monitor (para despertar y ver el estado de todos)")
    parser.add_argument("--email", help="Operador; por defecto ADMIN_EMAIL de despliegue.local/")
    parser.add_argument("--password", help="Clave; por defecto ADMIN_PASSWORD de despliegue.local/")
    parser.add_argument("--sin-login", action="store_true", help="Sin login (orquestador con AUTH_HABILITADA=false)")
    parser.add_argument("--sin-llm", action="store_true", help="El lead alcanza con quedar calificado (sin resumen)")
    parser.add_argument("--espera", type=float, default=600.0, help="Segundos máximos por flujo (default 600)")
    args = parser.parse_args(argv)

    email, password = (None, None) if args.sin_login else _credenciales_locales()
    email, password = args.email or email, args.password or password
    if not args.sin_login and not (email and password):
        parser.error("faltan --email y --password (o despliegue.local/ con ADMIN_EMAIL y ADMIN_PASSWORD)")

    with httpx.Client(timeout=httpx.Timeout(90.0, connect=30.0)) as http:
        verificador = Verificador(http, args.gateway, args.monitor, email, password, args.sin_llm, args.espera)
        ok = verificador.correr()
    fallas = [r for r in verificador.resultados if not r.ok]
    print("\nTODO OK" if ok else f"\n{len(fallas)} paso(s) fallaron: {', '.join(r.paso for r in fallas)}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
