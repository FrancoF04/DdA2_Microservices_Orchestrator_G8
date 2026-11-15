"""Conecta los mensajes de Kafka con la máquina de estados de los flujos.

El orquestador lee dos tópicos (docs/08-contrato-tres-topicos.md):

- `crm.eventos`: los hechos que publican los servicios. Según su `eventType` avanzan el
  flujo y pueden dejar anotados comandos para el paso siguiente (se publican en `crm.comandos`).
- `crm.dlt`: los comandos que un servicio no pudo procesar. Según el `eventType` del comando
  fallido se marca el paso o el flujo.

Por cada mensaje, en una sola transacción:
1. Lo registra en `evento_procesado` (si ya estaba, es un duplicado y se ignora).
2. Crea el flujo del lead si no existe (`ON CONFLICT DO NOTHING`).
3. Bloquea la fila del flujo (`FOR UPDATE`), aplica la transición y anota los comandos en el outbox.

Los mensajes sin sobre, o con un `eventType` que el orquestador no usa (ej. `ClienteActualizado`),
se ignoran sin tocar nada. El momento de cada mensaje es su timestamp en Kafka, así las latencias
entre pasos se miden con un mismo reloj.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from functools import partial
from typing import Any, Callable, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db.models import EventoProcesado, Flujo
from app.db.session import obtener_sessionmaker
from app.flujos import comandos as cmd
from app.flujos import registro_lead as rl
from app.flujos.comandos import ComandoSaliente
from app.flujos.despachador import encolar
from app.kafka.cliente import MANEJADORES
from app.kafka.eventos import extraer_payload

logger = logging.getLogger(__name__)

# Cómo se aplica un mensaje: recibe el flujo y devuelve los comandos que hay que ordenar después.
Regla = Callable[[Flujo, datetime, str, dict[str, Any]], Sequence[ComandoSaliente]]

# Eventos que usa el orquestador (docs/08 §4).
PREFORMULARIO_COMPLETADO = "PreformularioCompletado"
LEAD_CALIFICADO = "LeadCalificado"
RESUMEN_GENERADO = "ResumenGenerado"


def _momento(record: Any) -> datetime:
    ts = getattr(record, "timestamp", None)
    if ts:
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return datetime.now(timezone.utc)


def _clave_evento(mensaje: dict[str, Any], record: Any) -> str:
    """eventId si viene; si no, la posición en Kafka."""
    event_id = mensaje.get("eventId")
    if event_id:
        return str(event_id)
    return f"{record.topic}:{record.partition}:{record.offset}"


async def procesar_mensaje(reglas: dict[str, Regla], mensaje: dict[str, Any], record: Any) -> None:
    regla = reglas.get(str(mensaje.get("eventType")))
    if regla is None:
        logger.debug(
            "Mensaje %s de %s@%s: el orquestador no lo usa.", mensaje.get("eventType"), record.topic, record.offset
        )
        return
    payload = extraer_payload(mensaje)
    lead_id = payload.get("leadId")
    if not lead_id:
        logger.warning("Mensaje sin leadId en %s@%s: se ignora.", record.topic, record.offset)
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
            logger.info("Mensaje duplicado %s (%s): se ignora.", clave, record.topic)
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
        ordenes = regla(flujo, momento, clave, payload)
        for orden in ordenes:
            # En la misma transacción que avanza el flujo: o se guardan los dos o ninguno.
            await encolar(session, flujo, orden)
        logger.info(
            "Flujo %s lead=%s: %s → %s (%s %s)%s",
            rl.TIPO, lead_id, estado_antes, flujo.estado, record.topic, mensaje.get("eventType"),
            f", ordena {[o.event_type for o in ordenes]}" if ordenes else "",
        )


def reglas_de_eventos(settings: Settings) -> dict[str, Regla]:
    """Qué hace el orquestador con cada evento de `crm.eventos`."""

    def preformulario_completado(
        flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]
    ) -> list[ComandoSaliente]:
        primera_vez = rl.al_preformulario_completado(flujo, momento, ref, settings)
        return [cmd.calificar_lead(settings, payload)] if primera_vez else []

    def lead_calificado(
        flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]
    ) -> list[ComandoSaliente]:
        pedir_resumen = rl.al_lead_calificado(flujo, momento, ref, payload.get("origen"), settings)
        # La calificación (automática o corrección manual) la registran clientes y leads. El resumen se
        # pide una sola vez: una corrección manual no lo regenera (usa datos del lead, no la calificación).
        ordenes = [cmd.registrar_calificacion(settings, payload), cmd.actualizar_calificacion(settings, payload)]
        if pedir_resumen:
            ordenes.append(cmd.generar_resumen(settings))
        return ordenes

    def resumen_generado(
        flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]
    ) -> list[ComandoSaliente]:
        rl.al_resumen_generado(flujo, momento, ref, settings)
        if not payload.get("resumen"):
            logger.warning("ResumenGenerado del lead %s sin resumen: no hay nada que guardar.", payload.get("leadId"))
            return []
        return [cmd.guardar_resumen(settings, {"leadId": payload["leadId"], "resumen": payload["resumen"]})]

    return {
        PREFORMULARIO_COMPLETADO: preformulario_completado,
        LEAD_CALIFICADO: lead_calificado,
        RESUMEN_GENERADO: resumen_generado,
    }


def reglas_de_dlt(settings: Settings) -> dict[str, Regla]:
    """Qué hace el orquestador con cada comando que llega a `crm.dlt` (el `eventType` es el del comando fallido)."""

    def calificar_lead(flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]) -> list[ComandoSaliente]:
        rl.al_preformulario_en_dlt(flujo, momento, ref, settings)
        return []

    def generar_resumen(flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]) -> list[ComandoSaliente]:
        rl.al_resumen_en_dlt(flujo, momento, ref, settings)
        return []

    def con_error(tipo: str) -> Regla:
        paso, servicio = cmd.PASO_DEL_COMANDO[tipo]

        def aplicar(flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]) -> list[ComandoSaliente]:
            detalle = f"{servicio} no pudo procesar {tipo} (mensaje enviado a la DLT)"
            rl.al_comando_con_error(flujo, paso, servicio, momento, ref, detalle)
            return []

        return aplicar

    reglas: dict[str, Regla] = {cmd.CALIFICAR_LEAD: calificar_lead, cmd.GENERAR_RESUMEN: generar_resumen}
    reglas.update({tipo: con_error(tipo) for tipo in cmd.PASO_DEL_COMANDO})
    return reglas


def registrar_manejadores(settings: Settings) -> None:
    """Le dice al cliente de Kafka qué tópicos escuchar y qué hacer con cada uno."""
    MANEJADORES[settings.kafka_topico_eventos] = partial(procesar_mensaje, reglas_de_eventos(settings))
    MANEJADORES[settings.kafka_topico_dlt] = partial(procesar_mensaje, reglas_de_dlt(settings))


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
