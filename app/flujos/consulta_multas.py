"""Cómo se abre una consulta de multas: a pedido (`POST /api/orquestador/multas/{vehiculoId}`) o por el barrido periódico.

El orquestador decide cuándo y cuántas consultas abre; el scraper decide cómo consultar y vehiculos qué
significa tener multas. Abrir una consulta es anotar `ConsultarMultas` en el outbox (en la misma
transacción que deja el flujo en CONSULTANDO_MULTAS); el despachador lo publica en `crm.comandos`.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db.models import Flujo
from app.db.session import obtener_sessionmaker
from app.flujos import comandos as cmd
from app.flujos import multas
from app.flujos.despachador import ORIGEN, encolar
from app.proxy.clientes_http import ClientesHttp

logger = logging.getLogger(__name__)


async def abrir_consulta(
    session: AsyncSession, settings: Settings, vehiculo_id: str, correlation_id: Optional[str] = None
) -> Flujo:
    """Abre (o reabre) el flujo de la unidad y anota `ConsultarMultas`. Lanza `multas.ConsultaEnCurso` si ya hay una.

    Se llama dentro de una transacción: o queda el flujo y el comando, o ninguno.
    """
    await session.execute(
        pg_insert(Flujo)
        .values(
            id=uuid.uuid4(),
            tipo=multas.TIPO,
            clave_negocio=vehiculo_id,
            estado=multas.COMPLETADO,  # se pisa enseguida con `iniciar`; evita que un flujo nuevo parezca en curso
            correlation_id=correlation_id,
            intentos=0,
            version=1,
        )
        .on_conflict_do_nothing(constraint="uq_flujo_tipo_clave_negocio")
    )
    flujo = (
        await session.execute(
            select(Flujo)
            .where(Flujo.tipo == multas.TIPO, Flujo.clave_negocio == vehiculo_id)
            .options(selectinload(Flujo.pasos))
            .with_for_update()
        )
    ).scalar_one()
    # Cada consulta es una operación nueva: lleva su propio correlationId.
    flujo.correlation_id = correlation_id or str(uuid.uuid4())
    multas.iniciar(flujo, datetime.now(timezone.utc), flujo.correlation_id, settings)
    await encolar(session, flujo, cmd.consultar_multas(settings))
    return flujo


async def _unidades_disponibles(settings: Settings, http: ClientesHttp) -> list[str]:
    """Qué unidades corresponde consultar lo responde vehiculos: `GET /vehiculos?disponible=true`."""
    respuesta = await http.para("vehiculos").get(
        f"{settings.vehiculos_url.rstrip('/')}/vehiculos",
        params={"disponible": "true"},
        headers={"X-Origen-Servicio": ORIGEN},
        timeout=httpx.Timeout(settings.proxy_timeout_segundos, connect=settings.proxy_timeout_conexion_segundos),
    )
    if respuesta.status_code != 200:
        raise RuntimeError(f"vehiculos respondió {respuesta.status_code} al listar las unidades disponibles")
    return [str(v["id"]) for v in respuesta.json() if v.get("id")]


async def barrer_multas(settings: Settings, http: ClientesHttp) -> int:
    """Abre una consulta por cada unidad disponible que no tenga una en curso, hasta el tope. Devuelve cuántas abrió."""
    sessionmaker = obtener_sessionmaker()
    if sessionmaker is None:
        return 0
    abiertas = 0
    for vehiculo_id in await _unidades_disponibles(settings, http):
        if abiertas >= settings.multas_barrido_maximo:
            logger.info("Barrido de multas: se llegó al tope de %s consultas; el resto va en el próximo.", abiertas)
            break
        try:
            async with sessionmaker() as session, session.begin():
                await abrir_consulta(session, settings, vehiculo_id)
        except multas.ConsultaEnCurso:
            continue
        abiertas += 1
    return abiertas


async def barrer_periodicamente(settings: Settings, http: ClientesHttp) -> None:
    while True:
        try:
            abiertas = await barrer_multas(settings, http)
            logger.info("Barrido de multas: %s consultas abiertas.", abiertas)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - la tarea no puede morir por un error puntual
            logger.warning("No se pudo hacer el barrido de multas: %s", exc)
        await asyncio.sleep(settings.multas_barrido_intervalo_segundos)
