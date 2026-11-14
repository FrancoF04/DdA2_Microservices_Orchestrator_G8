"""Propagación del header `X-Correlation-Id`.

Mismo criterio que `CorrelationIdFilter` de vehiculos: se respeta el valor
entrante si es seguro (`[A-Za-z0-9._-]{1,64}`); si no viene o trae caracteres
raros se genera un UUID, para no dejar que el cliente inyecte líneas en los
logs. El valor queda disponible en un contextvar y se devuelve en la respuesta.
"""

from __future__ import annotations

import re
import uuid
from contextvars import ContextVar
from typing import Optional

from starlette.types import ASGIApp, Message, Receive, Scope, Send

HEADER = "X-Correlation-Id"
_VALIDO = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

_correlation_id: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def correlation_id_actual() -> Optional[str]:
    return _correlation_id.get()


def resolver_correlation_id(valor: Optional[str]) -> str:
    if valor and _VALIDO.match(valor):
        return valor
    return str(uuid.uuid4())


class CorrelationIdMiddleware:
    """Middleware ASGI puro (no bufferiza el body, apto para el proxy)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        entrante = None
        for nombre, valor in scope.get("headers", []):
            if nombre.decode("latin-1").lower() == HEADER.lower():
                entrante = valor.decode("latin-1")
                break

        cid = resolver_correlation_id(entrante)
        token = _correlation_id.set(cid)

        async def send_con_header(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = [
                    (k, v)
                    for k, v in message.get("headers", [])
                    if k.decode("latin-1").lower() != HEADER.lower()
                ]
                headers.append((HEADER.encode("latin-1"), cid.encode("latin-1")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_con_header)
        finally:
            _correlation_id.reset(token)
