"""Punto de entrada del orquestador (API Gateway + orquestador de sagas)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import health
from app.config import Settings, get_settings
from app.db.session import cerrar_db, iniciar_db
from app.errores import registrar_manejadores
from app.kafka.cliente import ClienteKafka
from app.logging_config import configurar_logging
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.middleware.correlation_id import CorrelationIdMiddleware


def crear_app(settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or get_settings()
    configurar_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        iniciar_db(settings)
        # Cliente HTTP compartido para el proxy (fase 1a).
        app.state.http = httpx.AsyncClient(
            timeout=settings.proxy_timeout_segundos, follow_redirects=False
        )
        await app.state.kafka.iniciar()
        try:
            yield
        finally:
            await app.state.kafka.detener()
            await app.state.http.aclose()
            await cerrar_db()

    app = FastAPI(
        title="Orquestador DdA2",
        description="API Gateway y orquestador de flujos del ecosistema de microservicios.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.kafka = ClienteKafka(settings)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=[CORRELATION_HEADER, "Location", "ETag"],
    )
    # Último en agregarse = el más externo: el correlation id envuelve todo.
    app.add_middleware(CorrelationIdMiddleware)

    registrar_manejadores(app)
    app.include_router(health.router)
    return app


app = crear_app()
