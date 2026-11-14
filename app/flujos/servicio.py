"""Conecta los eventos de Kafka con la máquina de estados de los flujos.

Por cada evento, en una sola transacción:
1. Lo registra en `evento_procesado` (si ya estaba, es un duplicado y se ignora).
2. Crea el flujo del lead si no existe (`ON CONFLICT DO NOTHING`).
3. Bloquea la fila del flujo (`FOR UPDATE`) y aplica la transición.

El momento de cada evento es el timestamp del mensaje en Kafka, así las
latencias entre pasos se miden con un mismo reloj.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import Any, Callable, Optional

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db.models import EventoProcesado, Flujo
from app.db.session import obtener_sessionmaker
from app.flujos import registro_lead as rl
from app.kafka.cliente import MANEJADORES
from app.kafka.eventos import extraer_payload

logger = logging.getLogger(__name__)


def _momento(record: Any) -> datetime:
    ts = getattr(record, "timestamp", None)
    if ts:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _clave_evento(mensaje: dict[str, Any], record: Any) -> str:
    """eventId si viene; si no (lead-calificado llega sin sobre), la posición en Kafka."""
    event_id = mensaje.get("eventId")
    if event_id:
        return str(event_id)
    return f"{record.topic}:{record.partition}:{record.offset}"


async def procesar_evento(
    aplicar: Callable[[Flujo, datetime, str, dict[str, Any]], None],
    mensaje: dict[str, Any],
    record: Any,
) -> None:
    payload = extraer_payload(mensaje)
    lead_id = payload.get("leadId")
    if not lead_id:
        logger.warning("Evento sin leadId en %s@%s: se ignora.", record.topic, record.offset)
        return

    sessionmaker = obtener_sessionmaker()
    if sessionmaker is None:
        return

    clave = _clave_evento(mensaje, record)
    momento = _momento(record)
    async with sessionmaker() as session, session.begin():
        nuevo = await session.execute(
            pg_insert(EventoProcesado)
            .values(clave=clave, topic=record.topic)
            .on_conflict_do_nothing()
            .returning(EventoProcesado.clave)
        )
        if nuevo.scalar() is None:
            logger.info("Evento duplicado %s (%s): se ignora.", clave, record.topic)
            return

        await session.execute(
            pg_insert(Flujo)
            .values(
                id=uuid.uuid4(),
                tipo=rl.TIPO,
                clave_negocio=str(lead_id),
                estado=rl.CALIFICACION_PENDIENTE,
                correlation_id=mensaje.get("correlationId"),
                intentos=0,
                version=1,
            )
            .on_conflict_do_nothing(constraint="uq_flujo_tipo_clave_negocio")
        )
        flujo = (
            await session.execute(
                select(Flujo)
                .where(Flujo.tipo == rl.TIPO, Flujo.clave_negocio == str(lead_id))
                .options(selectinload(Flujo.pasos))
                .with_for_update()
            )
        ).scalar_one()

        if not flujo.correlation_id and mensaje.get("correlationId"):
            flujo.correlation_id = mensaje["correlationId"]

        estado_antes = flujo.estado
        aplicar(flujo, momento, clave, payload)
        logger.info(
            "Flujo %s lead=%s: %s → %s (%s)",
            rl.TIPO, lead_id, estado_antes, flujo.estado, record.topic,
        )


def registrar_manejadores(settings: Settings) -> None:
    """Le dice al cliente de Kafka qué tópicos escuchar y qué hacer con cada uno."""

    def con(fn: Callable[..., None]) -> Callable[[Flujo, datetime, str, dict[str, Any]], None]:
        return lambda flujo, momento, ref, payload: fn(flujo, momento, ref, settings)

    def lead_calificado(flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]) -> None:
        rl.al_lead_calificado(flujo, momento, ref, payload.get("origen"), settings)

    acciones = {
        settings.kafka_topico_preformulario_completado: con(rl.al_preformulario_completado),
        settings.kafka_topico_lead_calificado: lead_calificado,
        settings.kafka_topico_resumen_solicitado: con(rl.al_resumen_solicitado),
        settings.kafka_topico_resumen_generado: con(rl.al_resumen_generado),
        settings.kafka_topico_preformulario_dlt: con(rl.al_preformulario_en_dlt),
    }
    for topico, aplicar in acciones.items():
        MANEJADORES[topico] = partial(procesar_evento, aplicar)


async def vencer_flujos(ahora: Optional[datetime] = None, limite: int = 100) -> int:
    """Marca como EXPIRADO los flujos cuyo paso actual venció. Devuelve cuántos."""
    sessionmaker = obtener_sessionmaker()
    if sessionmaker is None:
        return 0
    ahora = ahora or datetime.now(timezone.utc)
    async with sessionmaker() as session, session.begin():
        vencidos = (
            await session.execute(
                select(Flujo)
                .where(Flujo.estado.in_(rl.ESTADOS_EN_CURSO), Flujo.vence_en < ahora)
                .options(selectinload(Flujo.pasos))
                .order_by(Flujo.vence_en)
                .limit(limite)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
        for flujo in vencidos:
            estado_antes = flujo.estado
            rl.vencer(flujo, ahora)
            logger.warning(
                "Flujo %s lead=%s EXPIRADO (estaba en %s): %s",
                flujo.tipo, flujo.clave_negocio, estado_antes, flujo.ultimo_error,
            )
    return len(vencidos)


async def vencer_periodicamente(settings: Settings) -> None:
    while True:
        try:
            await vencer_flujos()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - la tarea no puede morir por un error puntual
            logger.warning("No se pudieron revisar los flujos vencidos: %s", exc)
        await asyncio.sleep(settings.flujo_intervalo_vencimiento_segundos)
