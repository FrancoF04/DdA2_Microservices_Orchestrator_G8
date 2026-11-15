"""Sobre común de los eventos de integración.

Formato del documento de arquitectura v1.1 (`version`, `occurredAt`) más
`timestamp`, que es lo que leen hoy los consumidores Python (leads, ia-resumen).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional


def armar_sobre(
    event_type: str,
    payload: dict[str, Any],
    correlation_id: Optional[str] = None,
    version: int = 1,
    destino: Optional[str] = None,
) -> dict[str, Any]:
    """`destino` va solo en los comandos: es el servicio que los tiene que atender."""
    ahora = datetime.now(timezone.utc).isoformat()
    sobre = {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "version": version,
        "occurredAt": ahora,
        "timestamp": ahora,
        "correlationId": correlation_id or str(uuid.uuid4()),
        "payload": payload,
    }
    if destino is not None:
        sobre["destino"] = destino
    return sobre


def extraer_payload(mensaje: dict[str, Any]) -> dict[str, Any]:
    """Acepta mensajes con sobre (`payload`) o planos, como calificacion (I1)."""
    payload = mensaje.get("payload")
    return payload if isinstance(payload, dict) else mensaje
