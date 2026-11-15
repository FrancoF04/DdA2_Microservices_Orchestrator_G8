"""Punto de entrada del orquestador (API Gateway + orquestador de flujos)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import flujos, health, proxy, servicios
from app.auditoria import RegistroAuditoria
from app.config import Settings, get_settings
from app.db.session import cerrar_db, db_configurada, iniciar_db
from app.flujos import despachador
from app.flujos import servicio as servicio_flujos
from app.errores import registrar_manejadores
from app.kafka.cliente import ClienteKafka
from app.logging_config import configurar_logging
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.middleware.correlation_id import CorrelationIdMiddleware
from app.proxy.clientes_http import ClientesHttp
from app.seguridad.autenticador import Autenticador, validar_configuracion_auth
from app.seguridad.revocaciones import Revocaciones

logger = logging.getLogger(__name__)


def crear_app(
    settings: Optional[Settings] = None,
    transporte_http: Optional[httpx.AsyncBaseTransport] = None,
) -> FastAPI:
    """`transporte_http` permite a los tests simular los servicios destino."""
    settings = settings or get_settings()
    configurar_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Falla rápido: sin JWT_SECRET el gateway no puede validar tokens y no debe quedar abierto.
        validar_configuracion_auth(settings)
        iniciar_db(settings)
        cargados = await app.state.autenticador.revocaciones.cargar()
        if cargados:
            logger.info("Tokens revocados recuperados de la base: %s", cargados)
        # Un cliente HTTP por servicio destino (aislamiento y autoreparación).
        app.state.http = ClientesHttp(transporte_http)
        await app.state.auditoria.iniciar()
        tareas: list[asyncio.Task] = []
        if db_configurada():
            # Seguimiento de flujos (fase 1b): sin base no hay dónde guardarlos.
            servicio_flujos.registrar_manejadores(settings)
            tareas.append(asyncio.create_task(servicio_flujos.vencer_periodicamente(settings)))
            if settings.orquestador_publica_resumen:
                # Tanda A: el orquestador publica resumen-solicitado (outbox + despachador).
                tareas.append(
                    asyncio.create_task(
                        despachador.despachar_periodicamente(settings, app.state.kafka, app.state.http)
                    )
                )
            else:
                logger.info("ORQUESTADOR_PUBLICA_RESUMEN=false: leads sigue publicando resumen-solicitado.")
        await app.state.kafka.iniciar()
        try:
            yield
        finally:
            for tarea in tareas:
                tarea.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await tarea
            await app.state.kafka.detener()
            await app.state.auditoria.detener()
            await app.state.http.cerrar()
            await cerrar_db()

    app = FastAPI(
        title="Orquestador DdA2",
        description="API Gateway y orquestador de flujos del ecosistema de microservicios.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.kafka = ClienteKafka(settings)
    app.state.auditoria = RegistroAuditoria(settings)
    app.state.autenticador = Autenticador(settings, Revocaciones())

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        # Retry-After: el front lo lee para decir cuánto falta cuando el login responde 429.
        expose_headers=[CORRELATION_HEADER, "Location", "ETag", "Retry-After"],
    )
    # Último en agregarse = el más externo: el correlation id envuelve todo.
    app.add_middleware(CorrelationIdMiddleware)

    registrar_manejadores(app)
    app.include_router(health.router)
    # Las rutas propias van antes que el proxy: /api/orquestador/** no se reenvía.
    app.include_router(servicios.router)
    app.include_router(flujos.router)
    app.include_router(proxy.router)
    return app


app = crear_app()
