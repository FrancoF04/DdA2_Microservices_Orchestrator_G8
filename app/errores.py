"""Errores en formato ProblemDetail (RFC 7807), igual que vehiculos y calificacion.

Cada respuesta de error lleva `codigo` (constante legible) y `correlationId`.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.middleware.correlation_id import correlation_id_actual

logger = logging.getLogger(__name__)

PROBLEM_JSON = "application/problem+json"

_CODIGOS_POR_STATUS = {
    400: "SOLICITUD_INVALIDA",
    401: "NO_AUTENTICADO",
    403: "NO_AUTORIZADO",
    404: "RECURSO_NO_ENCONTRADO",
    405: "METODO_NO_PERMITIDO",
    422: "VALIDACION",
    502: "SERVICIO_RECHAZO",
    503: "SERVICIO_NO_DISPONIBLE",
    504: "SERVICIO_TIMEOUT",
}


class ErrorOrquestador(Exception):
    """Error propio del orquestador con status, código y detalle."""

    def __init__(
        self,
        status: int,
        codigo: str,
        detalle: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(detalle)
        self.status = status
        self.codigo = codigo
        self.detalle = detalle
        self.extra = extra or {}


def problem_response(
    status: int,
    codigo: str,
    detalle: str,
    titulo: Optional[str] = None,
    extra: Optional[dict[str, Any]] = None,
) -> JSONResponse:
    cuerpo: dict[str, Any] = {
        "type": "about:blank",
        "title": titulo or codigo.replace("_", " ").capitalize(),
        "status": status,
        "detail": detalle,
        "codigo": codigo,
        "correlationId": correlation_id_actual(),
    }
    if extra:
        cuerpo.update(extra)
    return JSONResponse(status_code=status, content=cuerpo, media_type=PROBLEM_JSON)


def registrar_manejadores(app: FastAPI) -> None:
    @app.exception_handler(ErrorOrquestador)
    async def _orquestador(_: Request, exc: ErrorOrquestador) -> JSONResponse:
        return problem_response(exc.status, exc.codigo, exc.detalle, extra=exc.extra)

    @app.exception_handler(StarletteHTTPException)
    async def _http(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        codigo = _CODIGOS_POR_STATUS.get(exc.status_code, "ERROR")
        return problem_response(exc.status_code, codigo, str(exc.detail))

    @app.exception_handler(RequestValidationError)
    async def _validacion(_: Request, exc: RequestValidationError) -> JSONResponse:
        errores = [
            {
                "campo": ".".join(str(p) for p in err.get("loc", []) if p != "body"),
                "mensaje": err.get("msg", ""),
            }
            for err in exc.errors()
        ]
        return problem_response(
            422, "VALIDACION", "Datos inválidos.", extra={"errores": errores}
        )

    @app.exception_handler(Exception)
    async def _inesperado(_: Request, exc: Exception) -> JSONResponse:
        logger.exception("Error no controlado: %s", exc)
        return problem_response(500, "ERROR_INTERNO", "Error interno del orquestador.")
