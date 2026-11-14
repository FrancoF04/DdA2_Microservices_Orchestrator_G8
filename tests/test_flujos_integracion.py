"""Seguimiento de flujos contra un Postgres real (el del compose local).

Se saltean si no hay base: definir TEST_DATABASE_URL o tener levantado
`docker compose up -d` (usa el usuario `orquestador` en 127.0.0.1:5432) con
las migraciones aplicadas.
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import count

import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db import session as db
from app.db.models import EventoProcesado, Flujo
from app.flujos import registro_lead as rl
from app.flujos import servicio
from app.kafka.cliente import MANEJADORES

URL = os.getenv("TEST_DATABASE_URL", "postgresql://orquestador:orquestador@127.0.0.1:5432/postgres")
_offsets = count(1)


@pytest.fixture(scope="session")
def event_loop_policy():
    # psycopg async no funciona con el loop por defecto de Windows.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@dataclass
class Registro:
    """Imita un ConsumerRecord de aiokafka."""

    topic: str
    partition: int
    offset: int
    timestamp: int


def registro(topico: str, momento: datetime) -> Registro:
    return Registro(topico, 0, next(_offsets), int(momento.timestamp() * 1000))


@pytest.fixture
async def base():
    settings = Settings(_env_file=None, database_url=URL, kafka_enabled=False)
    db.iniciar_db(settings)
    try:
        if not await db.verificar_db(timeout_segundos=2):
            pytest.skip("No hay Postgres disponible para tests de integración.")
        async with db.obtener_sessionmaker()() as s:
            existe = (await s.execute(text("select to_regclass('orquestador.flujo')"))).scalar()
        if existe is None:
            pytest.skip("Faltan las migraciones (alembic upgrade head).")
        MANEJADORES.clear()
        servicio.registrar_manejadores(settings)
        yield settings
    finally:
        MANEJADORES.clear()
        await db.cerrar_db()


async def _flujo(lead_id: str) -> Flujo:
    async with db.obtener_sessionmaker()() as s:
        return (
            await s.execute(
                select(Flujo).where(Flujo.clave_negocio == lead_id).options(selectinload(Flujo.pasos))
            )
        ).scalar_one()


async def _limpiar(lead_id: str, claves: list[str]) -> None:
    async with db.obtener_sessionmaker()() as s, s.begin():
        await s.execute(delete(Flujo).where(Flujo.clave_negocio == lead_id))
        await s.execute(delete(EventoProcesado).where(EventoProcesado.clave.in_(claves)))


async def test_flujo_completo_por_kafka(base: Settings) -> None:
    lead = f"test-{uuid.uuid4()}"
    t0 = datetime.now(timezone.utc)
    eventos = [
        (base.kafka_topico_preformulario_completado,
         {"eventId": f"{lead}-1", "correlationId": "cid-test", "payload": {"leadId": lead}}, t0),
        # lead-calificado llega sin sobre, como lo publica calificacion
        (base.kafka_topico_lead_calificado, {"leadId": lead, "origen": "AUTOMATICA"}, t0 + timedelta(seconds=1)),
        (base.kafka_topico_resumen_solicitado, {"eventId": f"{lead}-3", "payload": {"leadId": lead}}, t0 + timedelta(seconds=2)),
        (base.kafka_topico_resumen_generado, {"eventId": f"{lead}-4", "payload": {"leadId": lead}}, t0 + timedelta(seconds=7)),
    ]
    claves = [f"{lead}-1", f"{lead}-3", f"{lead}-4"]
    try:
        for topico, mensaje, momento in eventos:
            reg = registro(topico, momento)
            if "eventId" not in mensaje:
                claves.append(f"{reg.topic}:{reg.partition}:{reg.offset}")
            await MANEJADORES[topico](mensaje, reg)

        flujo = await _flujo(lead)
        assert flujo.estado == rl.COMPLETADO
        assert flujo.correlation_id == "cid-test"
        latencias = {p.paso: p.latencia_ms for p in flujo.pasos}
        assert latencias == {rl.CALIFICAR_LEAD: 1000, rl.GENERAR_RESUMEN: 5000}
    finally:
        await _limpiar(lead, claves)


async def test_evento_duplicado_se_procesa_una_vez(base: Settings) -> None:
    lead = f"test-{uuid.uuid4()}"
    mensaje = {"eventId": f"{lead}-1", "payload": {"leadId": lead}}
    topico = base.kafka_topico_preformulario_completado
    try:
        await MANEJADORES[topico](mensaje, registro(topico, datetime.now(timezone.utc)))
        await MANEJADORES[topico](mensaje, registro(topico, datetime.now(timezone.utc)))

        flujo = await _flujo(lead)
        assert len(flujo.pasos) == 1
        assert flujo.version == 2  # una sola actualización después del alta
    finally:
        await _limpiar(lead, [f"{lead}-1"])


async def test_eventos_simultaneos_del_mismo_lead_no_duplican_el_flujo(base: Settings) -> None:
    lead = f"test-{uuid.uuid4()}"
    t0 = datetime.now(timezone.utc)
    pre, calif = base.kafka_topico_preformulario_completado, base.kafka_topico_lead_calificado
    try:
        await asyncio.gather(
            MANEJADORES[pre]({"eventId": f"{lead}-1", "payload": {"leadId": lead}}, registro(pre, t0)),
            MANEJADORES[calif]({"eventId": f"{lead}-2", "payload": {"leadId": lead}}, registro(calif, t0 + timedelta(seconds=1))),
        )
        async with db.obtener_sessionmaker()() as s:
            cantidad = (
                await s.execute(text("select count(*) from orquestador.flujo where clave_negocio = :l"), {"l": lead})
            ).scalar()
        assert cantidad == 1
        assert (await _flujo(lead)).estado == rl.CALIFICADO
    finally:
        await _limpiar(lead, [f"{lead}-1", f"{lead}-2"])


async def test_vencer_flujos_marca_los_trabados(base: Settings) -> None:
    lead = f"test-{uuid.uuid4()}"
    t0 = datetime.now(timezone.utc)
    topico = base.kafka_topico_preformulario_completado
    try:
        await MANEJADORES[topico]({"eventId": f"{lead}-1", "payload": {"leadId": lead}}, registro(topico, t0))

        await servicio.vencer_flujos(ahora=t0 + timedelta(seconds=base.flujo_timeout_calificacion_segundos + 1))

        flujo = await _flujo(lead)
        assert flujo.estado == rl.EXPIRADO
        assert flujo.pasos[0].estado == rl.TIMEOUT
    finally:
        await _limpiar(lead, [f"{lead}-1"])
