"""Publicación confiable de comandos: outbox + despachador.

El orquestador no publica en Kafka dentro del evento que lo dispara. Anota el
comando en la tabla `outbox` (en la misma transacción que avanza el flujo) y un
despachador lo publica aparte en `crm.comandos`:

    evento en crm.eventos ──► outbox (pendiente) ──despachador──► comando en crm.comandos

- Si Kafka o leads están caídos no se pierde nada: el comando sigue pendiente y se
  reintenta con espera creciente (`base * 2^(intento-1)`, con tope).
- Tras `outbox_max_intentos` fallos se abandona y el flujo queda FALLIDO (o el paso con
  error, según el comando); `GenerarResumen` se recupera con
  `POST /api/orquestador/flujos/{id}/reintentar`.
- Los comandos de un mismo lead salen en el orden en que se anotaron: uno no se
  publica mientras haya otro anterior pendiente (ej. `GuardarResumen` espera a `GenerarResumen`).
- Es *al menos una vez*: si el proceso cae entre publicar y confirmar, el comando
  se publica de nuevo. Los servicios lo toleran (son idempotentes por diseño).
- Varios despachadores a la vez no se pisan: cada fila se toma con `SKIP LOCKED`.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol

import httpx
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased, selectinload

from app.config import Settings
from app.db.models import Flujo, Outbox
from app.db.session import obtener_sessionmaker
from app.flujos import comandos as cmd
from app.flujos import registro_lead as rl
from app.flujos.comandos import ComandoSaliente
from app.flujos.resumen import armar_payload_resumen
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.proxy.clientes_http import ClientesHttp

logger = logging.getLogger(__name__)

ORIGEN = "orquestador"


class Publicador(Protocol):
    """Lo que el despachador necesita de `ClienteKafka` (los tests ponen un doble)."""

    disponible: bool

    async def publicar(
        self,
        topico: str,
        clave: str,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: Optional[str] = None,
        destino: Optional[str] = None,
    ) -> dict[str, Any]: ...


async def encolar(session: AsyncSession, flujo: Flujo, comando: ComandoSaliente) -> bool:
    """Anota el comando en el outbox. Con `unico`, no hace nada si ya hay uno igual pendiente (devuelve False).

    Se llama con la fila del flujo bloqueada, así dos eventos del mismo lead no encolan dos veces.
    """
    if comando.unico:
        pendiente = (
            await session.execute(
                select(Outbox.id).where(
                    Outbox.flujo_id == flujo.id,
                    Outbox.topic == comando.topic,
                    Outbox.mensaje["eventType"].as_string() == comando.event_type,
                    Outbox.publicado_en.is_(None),
                    Outbox.abandonado_en.is_(None),
                )
            )
        ).first()
        if pendiente is not None:
            return False
    mensaje: dict[str, Any] = {
        "eventType": comando.event_type,
        "destino": comando.destino,
        "leadId": flujo.clave_negocio,
        "correlationId": flujo.correlation_id,
    }
    if comando.payload is not None:
        mensaje["payload"] = comando.payload
    session.add(Outbox(flujo_id=flujo.id, topic=comando.topic, clave=flujo.clave_negocio, mensaje=mensaje))
    # Un flush por comando: el `orden` queda en el orden en que se anotaron.
    await session.flush()
    return True


async def _datos_del_lead(
    settings: Settings, http: ClientesHttp, lead_id: str, correlation_id: Optional[str]
) -> dict[str, Any]:
    """`GET /leads/{id}` directo a leads (el orquestador es el gateway: no se llama a sí mismo)."""
    headers = {"X-Origen-Servicio": ORIGEN}
    if correlation_id:
        headers[CORRELATION_HEADER] = correlation_id
    respuesta = await http.para("leads").get(
        f"{settings.leads_url.rstrip('/')}/leads/{lead_id}",
        headers=headers,
        timeout=httpx.Timeout(settings.proxy_timeout_segundos, connect=settings.proxy_timeout_conexion_segundos),
    )
    if respuesta.status_code != 200:
        raise RuntimeError(f"leads respondió {respuesta.status_code} al consultar el lead")
    return respuesta.json()


def _descripcion(exc: Exception) -> str:
    texto = str(exc)
    return f"{type(exc).__name__}: {texto}"[:500] if texto else type(exc).__name__


def _espera(settings: Settings, intentos: int) -> timedelta:
    segundos = settings.outbox_espera_base_segundos * (2 ** max(0, intentos - 1))
    return timedelta(seconds=min(segundos, settings.outbox_espera_maxima_segundos))


async def _flujo_bloqueado(session: AsyncSession, flujo_id: Any) -> Optional[Flujo]:
    if flujo_id is None:
        return None
    return (
        await session.execute(
            select(Flujo).where(Flujo.id == flujo_id).options(selectinload(Flujo.pasos)).with_for_update()
        )
    ).scalar_one_or_none()


def _sin_datos(mensaje: dict[str, Any]) -> dict[str, Any]:
    """El mismo mensaje sin el payload: ya publicado o abandonado no se guardan datos de negocio."""
    return {k: v for k, v in mensaje.items() if k != "payload"}


async def _payload(settings: Settings, http: ClientesHttp, mensaje: dict[str, Any]) -> dict[str, Any]:
    if mensaje["eventType"] == cmd.GENERAR_RESUMEN:
        lead = await _datos_del_lead(settings, http, mensaje["leadId"], mensaje.get("correlationId"))
        return armar_payload_resumen(lead)
    return mensaje["payload"]


def _al_abandonar(flujo: Flujo, tipo: str, ahora: datetime, referencia: str, detalle: str) -> None:
    if tipo == cmd.GENERAR_RESUMEN:
        rl.al_resumen_no_publicado(flujo, detalle)
    elif tipo == cmd.CALIFICAR_LEAD:
        rl.al_calificar_no_publicado(flujo, ahora, referencia, detalle)
    elif tipo in cmd.PASO_DEL_COMANDO:
        paso, servicio = cmd.PASO_DEL_COMANDO[tipo]
        rl.al_comando_con_error(flujo, paso, servicio, ahora, referencia, f"No se pudo publicar {tipo}: {detalle}")


async def _despachar_uno(
    session: AsyncSession, fila: Outbox, settings: Settings, kafka: Publicador, http: ClientesHttp, ahora: datetime
) -> bool:
    """Publica una fila. Devuelve True si se publicó."""
    mensaje = fila.mensaje
    tipo = mensaje["eventType"]
    try:
        payload = await _payload(settings, http, mensaje)
        sobre = await kafka.publicar(
            fila.topic, fila.clave, tipo, payload, mensaje.get("correlationId"), destino=mensaje["destino"]
        )
    except Exception as exc:  # noqa: BLE001 - cualquier falla es un intento fallido, no se pierde el comando
        fila.intentos += 1
        fila.ultimo_error = _descripcion(exc)
        if fila.intentos >= settings.outbox_max_intentos:
            fila.abandonado_en = ahora
            fila.mensaje = _sin_datos(mensaje)
            flujo = await _flujo_bloqueado(session, fila.flujo_id)
            if flujo is not None:
                _al_abandonar(flujo, tipo, ahora, str(fila.id), fila.ultimo_error)
            logger.error(
                "Abandonado %s del lead %s tras %s intentos: %s",
                tipo, fila.clave, fila.intentos, fila.ultimo_error,
            )
        else:
            fila.proximo_intento_en = ahora + _espera(settings, fila.intentos)
            logger.warning(
                "No se pudo publicar %s del lead %s (intento %s/%s): %s",
                tipo, fila.clave, fila.intentos, settings.outbox_max_intentos, fila.ultimo_error,
            )
        return False

    fila.publicado_en = ahora
    fila.ultimo_error = None
    fila.mensaje = _sin_datos(mensaje)
    if tipo == cmd.GENERAR_RESUMEN:
        # El pedido ya salió: el flujo pasa a esperar el resumen.
        flujo = await _flujo_bloqueado(session, fila.flujo_id)
        if flujo is not None:
            rl.al_resumen_solicitado(flujo, ahora, sobre["eventId"], settings)
    logger.info("Publicado %s del lead %s en %s (destino %s).", tipo, fila.clave, fila.topic, mensaje["destino"])
    return True


async def despachar_outbox(
    settings: Settings,
    kafka: Publicador,
    http: ClientesHttp,
    ahora: Optional[datetime] = None,
    limite: int = 20,
) -> int:
    """Publica los comandos pendientes que ya les toca. Devuelve cuántos publicó."""
    sessionmaker = obtener_sessionmaker()
    if sessionmaker is None or not kafka.disponible:
        return 0  # sin Kafka no se gastan intentos: el comando espera
    ahora = ahora or datetime.now(timezone.utc)
    publicados = 0
    anterior = aliased(Outbox)
    # Un comando espera a los anteriores del mismo lead que sigan pendientes.
    hay_anterior_pendiente = exists().where(
        anterior.clave == Outbox.clave,
        anterior.orden < Outbox.orden,
        anterior.publicado_en.is_(None),
        anterior.abandonado_en.is_(None),
    )
    for _ in range(limite):
        # Una transacción por fila: cada comando se confirma apenas se publica.
        async with sessionmaker() as session, session.begin():
            fila = (
                await session.execute(
                    select(Outbox)
                    .where(
                        Outbox.publicado_en.is_(None),
                        Outbox.abandonado_en.is_(None),
                        Outbox.proximo_intento_en <= ahora,
                        ~hay_anterior_pendiente,
                    )
                    .order_by(Outbox.orden)
                    .limit(1)
                    .with_for_update(skip_locked=True, of=Outbox)
                )
            ).scalar_one_or_none()
            if fila is None:
                break
            if await _despachar_uno(session, fila, settings, kafka, http, ahora):
                publicados += 1
    return publicados


async def despachar_periodicamente(settings: Settings, kafka: Publicador, http: ClientesHttp) -> None:
    while True:
        try:
            await despachar_outbox(settings, kafka, http)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - la tarea no puede morir por un error puntual
            logger.warning("No se pudo revisar el outbox: %s", exc)
        await asyncio.sleep(settings.outbox_intervalo_segundos)
