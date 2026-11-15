"""scripts/verificar_despliegue.py contra un ecosistema simulado (httpx.MockTransport)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Optional

import httpx
import pytest

RAIZ = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("verificar_despliegue", RAIZ / "scripts" / "verificar_despliegue.py")
vd = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = vd  # los dataclasses lo buscan ahí
_spec.loader.exec_module(vd)

GW = "https://gw.test"
MON = "https://mon.test"
UNIDAD = {"id": "11111111-1111-4111-8111-111111111111", "patente": "AAA111", "disponible": True}


class Ecosistema:
    """Responde como el gateway y el monitor. Los flujos avanzan un estado por cada consulta."""

    def __init__(self) -> None:
        self.salud = {"base": {"status": "UP"}, "kafka": {"status": "UP"}}
        self.estado_login = 200
        self.monitor = [{"servicio": "leads", "estado": "UP"}, {"servicio": "multas", "estado": "DEGRADADO"}]
        self.unidades: list[dict[str, Any]] = [{"id": "x", "patente": None}, dict(UNIDAD)]
        self.lead = ["CALIFICACION_PENDIENTE", "CALIFICADO", "RESUMEN_PENDIENTE", "COMPLETADO"]
        self.multas = ["CONSULTANDO_MULTAS", "ACTUALIZANDO_VEHICULO", "COMPLETADO"]
        self.estado_multas_post = 202
        self.pedidos: list[httpx.Request] = []

    def transporte(self) -> httpx.MockTransport:
        def manejar(r: httpx.Request) -> httpx.Response:
            self.pedidos.append(r)
            ruta = r.url.path
            if r.url.host == "mon.test":
                if ruta == "/monitor/despertar":
                    return httpx.Response(200, json=[])
                return httpx.Response(200, json=self.monitor)
            if ruta == "/health":
                return httpx.Response(200, json={"status": "UP", "componentes": self.salud})
            if ruta == "/api/auth/login":
                return httpx.Response(self.estado_login, json={"access_token": "tok", "refresh_token": "r"})
            if ruta == "/api/vehiculos":
                return httpx.Response(200, json=self.unidades)
            if ruta == "/api/leads":
                self.preformulario = json.loads(r.content)
                return httpx.Response(202, json={"lead_id": "lead-1"})
            if ruta.startswith("/api/orquestador/multas/"):
                return httpx.Response(self.estado_multas_post, json={})
            if ruta == "/api/orquestador/flujos":
                cola = self.lead if r.url.params["tipo"] == "REGISTRO_LEAD" else self.multas
                estado = cola.pop(0) if len(cola) > 1 else cola[0]
                return httpx.Response(200, json=[{"estado": estado, "pasos": [{"paso": "X", "estado": "OK"}],
                                                  "ultimoError": None}])
            return httpx.Response(404)

        return httpx.MockTransport(manejar)


def _verificador(eco: Ecosistema, **opciones: Any) -> Any:
    reloj = {"t": 0.0}

    def dormir(segundos: float) -> None:
        reloj["t"] += segundos

    valores = dict(monitor=MON, email="admin@dda2-crm.com", password="clave", espera_maxima=60.0, intervalo=5.0,
                   dormir=dormir, reloj=lambda: reloj["t"])
    valores.update(opciones)
    return vd.Verificador(httpx.Client(transport=eco.transporte()), GW, **valores)


def _resultados(v: Any) -> dict[str, bool]:
    return {r.paso: r.ok for r in v.resultados}


def test_un_ecosistema_sano_pasa_todos_los_pasos() -> None:
    eco = Ecosistema()
    v = _verificador(eco)

    assert v.correr() is True
    assert _resultados(v) == {
        "despertar servicios": True,
        "salud del orquestador": True,
        "login del operador": True,
        "estado según el monitor": True,
        "unidades disponibles": True,
        "lead de punta a punta": True,
        "multas de punta a punta": True,
    }


def test_usa_la_unidad_de_prueba_aaa111_y_manda_el_token_en_las_rutas_del_operador() -> None:
    eco = Ecosistema()
    eco.unidades = [{"id": "otra", "patente": "ZZZ000"}, dict(UNIDAD)]
    _verificador(eco).correr()

    multas = [p for p in eco.pedidos if p.url.path.startswith("/api/orquestador/multas/")]
    assert multas[0].url.path.endswith(UNIDAD["id"])
    assert multas[0].headers["Authorization"] == "Bearer tok"
    flujos = [p for p in eco.pedidos if p.url.path == "/api/orquestador/flujos"]
    assert all(p.headers["Authorization"] == "Bearer tok" for p in flujos)
    assert eco.preformulario["vehiculoId"] == UNIDAD["id"]


def test_el_preformulario_tiene_un_email_distinto_cada_vez() -> None:
    a, b = Ecosistema(), Ecosistema()
    _verificador(a).correr()
    _verificador(b).correr()

    assert a.preformulario["cliente"]["email"] != b.preformulario["cliente"]["email"]


def test_si_el_gateway_tiene_kafka_caido_falla_y_no_sigue() -> None:
    eco = Ecosistema()
    eco.salud["kafka"] = {"status": "DOWN"}
    v = _verificador(eco)

    assert v.correr() is False
    assert _resultados(v)["salud del orquestador"] is False
    assert "lead de punta a punta" not in _resultados(v)


def test_si_el_login_falla_no_sigue() -> None:
    eco = Ecosistema()
    eco.estado_login = 401
    v = _verificador(eco)

    assert v.correr() is False
    assert "unidades disponibles" not in _resultados(v)


def test_un_servicio_caido_en_el_monitor_es_una_falla_pero_lento_no() -> None:
    eco = Ecosistema()
    eco.monitor = [{"servicio": "leads", "estado": "DOWN"}, {"servicio": "multas", "estado": "DEGRADADO"}]
    v = _verificador(eco)

    assert v.correr() is False
    assert _resultados(v)["estado según el monitor"] is False
    assert "leads" in next(r.detalle for r in v.resultados if r.paso == "estado según el monitor")


def test_un_flujo_que_queda_fallido_es_una_falla() -> None:
    eco = Ecosistema()
    eco.multas = ["CONSULTANDO_MULTAS", "FALLIDO"]
    v = _verificador(eco)

    assert v.correr() is False
    assert _resultados(v)["multas de punta a punta"] is False
    assert _resultados(v)["lead de punta a punta"] is True


def test_un_flujo_que_no_termina_a_tiempo_es_una_falla() -> None:
    eco = Ecosistema()
    eco.lead = ["CALIFICACION_PENDIENTE"]  # nunca avanza
    v = _verificador(eco, espera_maxima=30.0)

    assert v.correr() is False
    assert "a tiempo" in next(r.detalle for r in v.resultados if r.paso == "lead de punta a punta")


def test_sin_llm_alcanza_con_que_el_lead_quede_calificado() -> None:
    eco = Ecosistema()
    eco.lead = ["CALIFICACION_PENDIENTE", "RESUMEN_PENDIENTE"]  # ia-resumen nunca responde
    v = _verificador(eco, sin_llm=True)

    assert v.correr() is True


def test_una_consulta_de_multas_ya_en_curso_se_sigue_igual() -> None:
    eco = Ecosistema()
    eco.estado_multas_post = 409  # la abrió el barrido o el alta
    v = _verificador(eco)

    assert v.correr() is True


def test_sin_unidades_con_patente_falla_y_no_crea_el_lead() -> None:
    eco = Ecosistema()
    eco.unidades = [{"id": "x", "patente": None}]
    v = _verificador(eco)

    assert v.correr() is False
    assert not any(p.url.path == "/api/leads" for p in eco.pedidos)


def test_sin_monitor_se_saltean_sus_pasos() -> None:
    v = _verificador(Ecosistema(), monitor=None)

    assert v.correr() is True
    assert "despertar servicios" not in _resultados(v)


def test_main_exige_credenciales_salvo_con_sin_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vd, "_credenciales_locales", lambda: (None, None))
    with pytest.raises(SystemExit):
        vd.main(["--gateway", GW])


def test_el_apellido_de_prueba_solo_tiene_letras_porque_clientes_rechaza_los_digitos() -> None:
    eco = Ecosistema()
    _verificador(eco).correr()

    apellido = eco.preformulario["cliente"]["apellido"]
    assert apellido.replace(" ", "").isalpha(), apellido


def test_si_leads_rechaza_el_preformulario_se_ve_el_campo() -> None:
    eco = Ecosistema()
    original = eco.transporte()

    def manejar(r: httpx.Request) -> httpx.Response:
        if r.url.path == "/api/leads":
            return httpx.Response(422, json={"codigo": "VALIDACION", "errores": [{"campo": "apellido", "mensaje": "solo letras"}]})
        return original.handle_request(r)

    v = vd.Verificador(httpx.Client(transport=httpx.MockTransport(manejar)), GW, monitor=None, email="a@b.com",
                       password="x", dormir=lambda s: None)
    v.correr()

    detalle = next(r.detalle for r in v.resultados if r.paso == "lead de punta a punta")
    assert "apellido: solo letras" in detalle
