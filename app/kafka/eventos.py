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
) -> dict[str, Any]:
    ahora = datetime.now(timezone.utc).isoformat()
    return {
        "eventId": str(uuid.uuid4()),
        "eventType": event_type,
        "version": version,
        "occurredAt": ahora,
        "timestamp": ahora,
        "correlationId": correlation_id or str(uuid.uuid4()),
        "payload": payload,
    }


def extraer_payload(mensaje: dict[str, Any]) -> dict[str, Any]:
    """Acepta mensajes con sobre (`payload`) o planos, como calificacion (I1)."""
    payload = mensaje.get("payload")
    return payload if isinstance(payload, dict) else mensaje
