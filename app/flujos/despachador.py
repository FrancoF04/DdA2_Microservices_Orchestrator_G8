"""Publicación confiable de comandos: outbox + despachador.

El orquestador no publica en Kafka dentro del evento que lo dispara. Anota el
comando en la tabla `outbox` (en la misma transacción que avanza el flujo) y un
despachador lo publica aparte:

    lead-calificado ──► outbox (pendiente) ──despachador──► resumen-solicitado

- Si Kafka o leads están caídos no se pierde nada: el comando sigue pendiente y se
  reintenta con espera creciente (`base * 2^(intento-1)`, con tope).
- Tras `outbox_max_intentos` fallos se abandona y el flujo queda FALLIDO; se
  recupera con `POST /api/orquestador/flujos/{id}/reintentar`.
- Es *al menos una vez*: si el proceso cae entre publicar y confirmar, el comando
  se publica de nuevo. ia-resumen y leads toleran el repetido (el resumen se
  sobreescribe).
- Varios despachadores a la vez no se pisan: cada fila se toma con `SKIP LOCKED`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db.models import Flujo, Outbox
from app.db.session import obtener_sessionmaker
from app.flujos import registro_lead as rl
from app.flujos.resumen import armar_payload_resumen
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.proxy.clientes_http import ClientesHttp

logger = logging.getLogger(__name__)

EVENTO_RESUMEN_SOLICITADO = "ResumenSolicitado"
ORIGEN = "orquestador"


class Publicador(Protocol):
    """Lo que el despachador necesita de `ClienteKafka` (los tests ponen un doble)."""

    disponible: bool

    async def publicar(
        self, topico: str, clave: str, event_type: str, payload: dict[str, Any], correlation_id: Optional[str] = None
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ComandoSaliente:
    """Lo que un evento le pide al orquestador que publique después."""

    topic: str
    event_type: str


async def encolar(session: AsyncSession, flujo: Flujo, comando: ComandoSaliente) -> bool:
    """Anota el comando en el outbox. No hace nada si ya hay uno igual pendiente (devuelve False).

    Se llama con la fila del flujo bloqueada, así dos eventos del mismo lead no encolan dos veces.
    """
    pendiente = (
        await session.execute(
            select(Outbox.id).where(
                Outbox.flujo_id == flujo.id,
                Outbox.topic == comando.topic,
                Outbox.publicado_en.is_(None),
                Outbox.abandonado_en.is_(None),
            )
        )
    ).first()
    if pendiente is not None:
        return False
    session.add(
        Outbox(
            flujo_id=flujo.id,
            topic=comando.topic,
            clave=flujo.clave_negocio,
            mensaje={
                "eventType": comando.event_type,
                "leadId": flujo.clave_negocio,
                "correlationId": flujo.correlation_id,
            },
        )
    )
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


async def _despachar_uno(
    session: AsyncSession, fila: Outbox, settings: Settings, kafka: Publicador, http: ClientesHttp, ahora: datetime
) -> bool:
    """Publica una fila. Devuelve True si se publicó."""
    correlation_id = fila.mensaje.get("correlationId")
    try:
        lead = await _datos_del_lead(settings, http, fila.mensaje["leadId"], correlation_id)
        payload = armar_payload_resumen(lead)
        sobre = await kafka.publicar(fila.topic, fila.clave, fila.mensaje["eventType"], payload, correlation_id)
    except Exception as exc:  # noqa: BLE001 - cualquier falla es un intento fallido, no se pierde el comando
        fila.intentos += 1
        fila.ultimo_error = _descripcion(exc)
        if fila.intentos >= settings.outbox_max_intentos:
            fila.abandonado_en = ahora
            flujo = await _flujo_bloqueado(session, fila.flujo_id)
            if flujo is not None:
                rl.al_resumen_no_publicado(flujo, fila.ultimo_error)
            logger.error(
                "Abandonado %s del lead %s tras %s intentos: %s",
                fila.mensaje["eventType"], fila.clave, fila.intentos, fila.ultimo_error,
            )
        else:
            fila.proximo_intento_en = ahora + _espera(settings, fila.intentos)
            logger.warning(
                "No se pudo publicar %s del lead %s (intento %s/%s): %s",
                fila.mensaje["eventType"], fila.clave, fila.intentos, settings.outbox_max_intentos, fila.ultimo_error,
            )
        return False

    fila.publicado_en = ahora
    fila.ultimo_error = None
    flujo = await _flujo_bloqueado(session, fila.flujo_id)
    if flujo is not None:
        rl.al_resumen_solicitado(flujo, ahora, sobre["eventId"], settings)
    logger.info("Publicado %s del lead %s en %s.", fila.mensaje["eventType"], fila.clave, fila.topic)
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
                    )
                    .order_by(Outbox.creado_en)
                    .limit(1)
                    .with_for_update(skip_locked=True)
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
