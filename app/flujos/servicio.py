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
from dataclasses import dataclass
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
from app.flujos import multas
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
MULTAS_CONSULTADAS = "MultasConsultadas"
CONSULTA_MULTAS_FALLIDA = "ConsultaMultasFallida"


@dataclass(frozen=True)
class DeQueFlujo:
    """A qué flujo pertenece un mensaje: su tipo, el campo del payload con la clave de negocio y si nace con él."""

    tipo: str
    campo: str
    estado_inicial: str
    # Los mensajes de un lead pueden llegar fuera de orden y abrir el flujo; los de multas solo
    # responden a una consulta que el orquestador ya abrió.
    crea_si_falta: bool


_DE_UN_LEAD = DeQueFlujo(rl.TIPO, "leadId", rl.CALIFICACION_PENDIENTE, crea_si_falta=True)
_DE_UN_VEHICULO = DeQueFlujo(multas.TIPO, "vehiculoId", multas.CONSULTANDO_MULTAS, crea_si_falta=False)
# Por `eventType` (del evento o del comando fallido en la DLT); lo que no figura es de un lead.
_FLUJO_DEL_MENSAJE = {
    MULTAS_CONSULTADAS: _DE_UN_VEHICULO,
    CONSULTA_MULTAS_FALLIDA: _DE_UN_VEHICULO,
    cmd.CONSULTAR_MULTAS: _DE_UN_VEHICULO,
}


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
    tipo_mensaje = str(mensaje.get("eventType"))
    regla = reglas.get(tipo_mensaje)
    if regla is None:
        logger.debug("Mensaje %s de %s@%s: el orquestador no lo usa.", tipo_mensaje, record.topic, record.offset)
        return
    de = _FLUJO_DEL_MENSAJE.get(tipo_mensaje, _DE_UN_LEAD)
    payload = extraer_payload(mensaje)
    lead_id = payload.get(de.campo)
    if not lead_id:
        logger.warning("Mensaje %s sin %s en %s@%s: se ignora.", tipo_mensaje, de.campo, record.topic, record.offset)
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

        if de.crea_si_falta:
            await session.execute(
                pg_insert(Flujo)
                .values(
                    id=uuid.uuid4(),
                    tipo=de.tipo,
                    clave_negocio=str(lead_id),
                    estado=de.estado_inicial,
                    correlation_id=mensaje.get("correlationId"),
                    intentos=0,
                    version=1,
                )
                .on_conflict_do_nothing(constraint="uq_flujo_tipo_clave_negocio")
            )
        flujo = (
            await session.execute(
                select(Flujo)
                .where(Flujo.tipo == de.tipo, Flujo.clave_negocio == str(lead_id))
                .options(selectinload(Flujo.pasos))
                .with_for_update()
            )
        ).scalar_one_or_none()
        if flujo is None:
            logger.warning("Mensaje %s de %s %s sin flujo abierto: se ignora.", tipo_mensaje, de.campo, lead_id)
            return

        if not flujo.correlation_id and mensaje.get("correlationId"):
            flujo.correlation_id = mensaje["correlationId"]

        estado_antes = flujo.estado
        ordenes = regla(flujo, momento, clave, payload)
        for orden in ordenes:
            # En la misma transacción que avanza el flujo: o se guardan los dos o ninguno.
            await encolar(session, flujo, orden)
        logger.info(
            "Flujo %s %s=%s: %s → %s (%s %s)%s",
            de.tipo, de.campo, lead_id, estado_antes, flujo.estado, record.topic, tipo_mensaje,
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

    def multas_consultadas(
        flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]
    ) -> list[ComandoSaliente]:
        if not multas.al_multas_consultadas(flujo, momento, ref, settings):
            return []
        # El resultado es dato personal: viaja a vehiculos y no queda en el flujo (solo la cantidad, en el log).
        logger.info("Multas de la unidad %s: %s actas.", payload["vehiculoId"], len(payload.get("infracciones") or []))
        return [cmd.guardar_multas({k: v for k, v in payload.items() if k != "vehiculoId"})]

    def consulta_multas_fallida(
        flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]
    ) -> list[ComandoSaliente]:
        motivo = str(payload.get("motivo") or "desconocido")
        # Sin el campo, se trata como no reintentable: el sitio puede haber cambiado y reintentar no lo arregla.
        reintentable = payload.get("reintentable") is True
        espera = multas.al_consulta_fallida(flujo, momento, ref, motivo, reintentable, settings)
        return [cmd.consultar_multas(settings, espera)] if espera is not None else []

    return {
        PREFORMULARIO_COMPLETADO: preformulario_completado,
        LEAD_CALIFICADO: lead_calificado,
        RESUMEN_GENERADO: resumen_generado,
        MULTAS_CONSULTADAS: multas_consultadas,
        CONSULTA_MULTAS_FALLIDA: consulta_multas_fallida,
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

    def consultar_multas(flujo: Flujo, momento: datetime, ref: str, payload: dict[str, Any]) -> list[ComandoSaliente]:
        multas.al_consulta_en_dlt(flujo, momento, ref)
        return []

    reglas: dict[str, Regla] = {
        cmd.CALIFICAR_LEAD: calificar_lead,
        cmd.GENERAR_RESUMEN: generar_resumen,
        cmd.CONSULTAR_MULTAS: consultar_multas,
    }
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
                .where(
                    Flujo.estado.in_(set(rl.ESTADOS_EN_CURSO) | set(multas.ESTADOS_EN_CURSO)),
                    Flujo.vence_en < ahora,
                )
                .options(selectinload(Flujo.pasos))
                .order_by(Flujo.vence_en)
                .limit(limite)
                .with_for_update(skip_locked=True)
            )
        ).scalars().all()
        for flujo in vencidos:
            estado_antes = flujo.estado
            (multas.vencer if flujo.tipo == multas.TIPO else rl.vencer)(flujo, ahora)
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
