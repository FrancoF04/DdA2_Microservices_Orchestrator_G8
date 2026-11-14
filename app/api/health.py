"""Salud propia del orquestador.

Formato compatible con el monitor (que lee `status` al estilo Actuator):
- `UP`: la app responde y ninguna dependencia configurada está caída.
- `DEGRADADO`: la app responde pero la base o Kafka están caídos.

Siempre responde 200 mientras el proceso viva: el healthcheck de Docker mide
el proceso; el detalle de las dependencias va en `componentes`.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.db import session as db
from app.kafka import cliente as kafka

router = APIRouter(tags=["Salud"])


@router.get("/health", summary="Estado del orquestador y sus dependencias")
async def health(request: Request) -> dict[str, Any]:
    if db.db_configurada():
        estado_db = kafka.UP if await db.verificar_db() else kafka.DOWN
    else:
        estado_db = kafka.DESHABILITADO

    cliente_kafka: kafka.ClienteKafka = request.app.state.kafka
    componente_kafka: dict[str, Any] = {"status": cliente_kafka.estado}
    if cliente_kafka.ultimo_error:
        componente_kafka["detalle"] = cliente_kafka.ultimo_error

    caidos = [estado_db, cliente_kafka.estado]
    status = "DEGRADADO" if kafka.DOWN in caidos else "UP"

    return {
        "status": status,
        "servicio": request.app.state.settings.app_nombre,
        "componentes": {
            "db": {"status": estado_db},
            "kafka": componente_kafka,
        },
    }
