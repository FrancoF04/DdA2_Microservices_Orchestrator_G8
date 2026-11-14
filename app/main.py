"""Punto de entrada del orquestador (API Gateway + orquestador de flujos)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api import health, proxy, servicios
from app.auditoria import RegistroAuditoria
from app.config import Settings, get_settings
from app.db.session import cerrar_db, iniciar_db
from app.errores import registrar_manejadores
from app.kafka.cliente import ClienteKafka
from app.logging_config import configurar_logging
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.middleware.correlation_id import CorrelationIdMiddleware
from app.proxy.clientes_http import ClientesHttp


def crear_app(
    settings: Optional[Settings] = None,
    transporte_http: Optional[httpx.AsyncBaseTransport] = None,
) -> FastAPI:
    """`transporte_http` permite a los tests simular los servicios destino."""
    settings = settings or get_settings()
    configurar_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        iniciar_db(settings)
        # Un cliente HTTP por servicio destino (aislamiento y autoreparación).
        app.state.http = ClientesHttp(transporte_http)
        await app.state.auditoria.iniciar()
        await app.state.kafka.iniciar()
        try:
            yield
        finally:
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
    # Las rutas propias van antes que el proxy: /api/orquestador/** no se reenvía.
    app.include_router(servicios.router)
    app.include_router(proxy.router)
    return app


app = crear_app()
