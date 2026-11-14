"""Simulador del microservicio `clientes` (SOLO para desarrollo local).

Existe porque el servicio real todavía no está en el ecosistema y, sin él,
`POST /leads` falla (leads da de alta el cliente antes de guardar el lead).
Responde lo mismo que esperan los demás servicios (ver
docs/01-relevamiento-ecosistema.md §3.7). Guarda todo en memoria: se pierde al
reiniciar. Cuando el servicio real esté disponible, se apaga este y listo.

Endpoints:
- POST /clientes                          (lo llama leads, con X-Api-Key)
- GET  /clientes/{id}                     (front, ia-resumen)
- GET  /clientes/{id}/situacion-crediticia (calificacion, con X-Api-Key)
- GET  /clientes/{id}/historial           (front)
- GET  /actuator/health                   (monitor)

La situación crediticia es determinística para poder probar las reglas de
calificación: sin CUIL → null ("sin datos"); con CUIL → (penúltimo dígito % 6) + 1.
Ej.: CUIL terminado en ...06 → 1, ...36 → 4.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger("clientes-simulado")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# Si se definen, se exigen como X-Api-Key (igual que el servicio real). Vacías = no se valida.
CLAVE_LEADS = os.getenv("CLIENTES_CLAVE_LEADS", "")
CLAVE_CALIFICACION = os.getenv("CLIENTES_CLAVE_CALIFICACION", "")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
TOPICO_LEAD_CALIFICADO = os.getenv("KAFKA_TOPIC_LEAD_CALIFICADO", "lead-calificado")

_clientes: dict[str, dict[str, Any]] = {}
_historial: dict[str, list[dict[str, Any]]] = {}


def _validar_clave(recibida: Optional[str], esperada: str) -> None:
    if esperada and recibida != esperada:
        raise HTTPException(status_code=401, detail="X-Api-Key inválida")


def _enmascarar(cuil: Optional[str]) -> Optional[str]:
    if not cuil:
        return None
    return f"{cuil[:2]}-****{cuil[-3:-1]}-{cuil[-1]}" if len(cuil) >= 11 else "****"


def _situacion(cuil: Optional[str]) -> Optional[int]:
    digitos = "".join(c for c in (cuil or "") if c.isdigit())
    if len(digitos) < 2:
        return None
    return int(digitos[-2]) % 6 + 1


async def _consumir_lead_calificado() -> None:
    """Registra el historial a partir de lead-calificado, como hará el servicio real."""
    from aiokafka import AIOKafkaConsumer

    while True:
        try:
            consumer = AIOKafkaConsumer(
                TOPICO_LEAD_CALIFICADO,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="clientes-simulado",
                auto_offset_reset="earliest",
            )
            await consumer.start()
            logger.info("Escuchando %s en %s", TOPICO_LEAD_CALIFICADO, KAFKA_BOOTSTRAP)
            try:
                async for msg in consumer:
                    try:
                        valor = json.loads(msg.value)
                        datos = valor.get("payload", valor)  # calificacion lo publica sin sobre
                        cliente_id = datos.get("clienteId")
                        if cliente_id:
                            _historial.setdefault(cliente_id, []).append(
                                {
                                    "leadId": datos.get("leadId"),
                                    "nivel": datos.get("nivel"),
                                    "puntaje": datos.get("puntos", datos.get("puntaje")),
                                    "origen": datos.get("origen"),
                                    "motivos": datos.get("motivos", []),
                                    "calculadaEn": datos.get("fecha") or datos.get("calculadaEn"),
                                }
                            )
                            logger.info("Historial: cliente %s, lead %s, nivel %s",
                                        cliente_id, datos.get("leadId"), datos.get("nivel"))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Mensaje de lead-calificado inválido: %s", exc)
            finally:
                await consumer.stop()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("Kafka no disponible (%s); reintento en 10 s", exc)
            await asyncio.sleep(10)


@asynccontextmanager
async def lifespan(app: FastAPI):
    tarea = asyncio.create_task(_consumir_lead_calificado()) if KAFKA_BOOTSTRAP else None
    yield
    if tarea:
        tarea.cancel()


app = FastAPI(title="clientes (simulado)", lifespan=lifespan)


@app.post("/clientes", status_code=201)
async def crear_cliente(request: Request, x_api_key: Optional[str] = Header(None)) -> Any:
    _validar_clave(x_api_key, CLAVE_LEADS)
    datos = await request.json()
    faltantes = [c for c in ("nombre", "apellido", "email", "provincia") if not datos.get(c)]
    if datos.get("cuil") and datos.get("consentimiento") is not True:
        faltantes.append("consentimiento")
    if faltantes:
        return JSONResponse(
            status_code=422,
            content={
                "type": "about:blank",
                "title": "Validación",
                "status": 422,
                "detail": "Datos del cliente inválidos.",
                "codigo": "VALIDACION",
                "errores": [{"campo": c, "mensaje": "Campo obligatorio."} for c in faltantes],
            },
        )
    cliente_id = str(uuid.uuid4())
    _clientes[cliente_id] = {**datos, "id": cliente_id, "creadoEn": datetime.now(timezone.utc).isoformat()}
    logger.info("Cliente creado: %s (%s %s)", cliente_id, datos["nombre"], datos["apellido"])
    return {"clienteId": cliente_id}


def _obtener(cliente_id: str) -> dict[str, Any]:
    cliente = _clientes.get(cliente_id)
    if cliente is None:
        raise HTTPException(status_code=404, detail="Cliente no encontrado")
    return cliente


@app.get("/clientes/{cliente_id}")
async def obtener_cliente(cliente_id: str) -> dict[str, Any]:
    c = _obtener(cliente_id)
    return {
        "id": c["id"],
        "nombre": c.get("nombre"),
        "apellido": c.get("apellido"),
        "email": c.get("email"),
        "telefono": c.get("telefono"),
        "provincia": c.get("provincia"),
        "localidad": c.get("localidad"),
        "canalOrigen": c.get("canalOrigen"),
        "cuil": _enmascarar(c.get("cuil")),
    }


@app.get("/clientes/{cliente_id}/situacion-crediticia")
async def situacion_crediticia(cliente_id: str, x_api_key: Optional[str] = Header(None)) -> dict[str, Any]:
    _validar_clave(x_api_key, CLAVE_CALIFICACION)
    c = _obtener(cliente_id)
    return {"clienteId": cliente_id, "situacion": _situacion(c.get("cuil"))}


@app.get("/clientes/{cliente_id}/historial")
async def historial(cliente_id: str) -> dict[str, Any]:
    _obtener(cliente_id)
    return {"clienteId": cliente_id, "calificaciones": _historial.get(cliente_id, [])}


@app.get("/actuator/health")
async def health() -> dict[str, str]:
    return {"status": "UP"}
