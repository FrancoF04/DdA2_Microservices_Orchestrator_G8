"""`/api/{recurso}/**`: puerta de entrada única hacia los microservicios."""

from __future__ import annotations

from fastapi import APIRouter, Request
from starlette.responses import Response

from app.errores import problem_response
from app.proxy.reenvio import reenviar
from app.proxy.rutas import RECURSOS, resolver_destino

router = APIRouter(tags=["Proxy"])

_METODOS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


async def _proxy(request: Request, recurso: str) -> Response:
    settings = request.app.state.settings
    destino = resolver_destino(recurso, settings)
    if destino is None:
        return problem_response(
            404,
            "RECURSO_NO_ENCONTRADO",
            f"No hay ningún servicio para '/api/{recurso}'.",
            extra={"recursosDisponibles": sorted(RECURSOS)},
        )
    # Se reenvía la ruta sin el prefijo /api (el recurso queda: /leads/..., /vehiculos/...).
    ruta = request.url.path.removeprefix("/api")
    return await reenviar(
        request,
        destino,
        ruta,
        request.app.state.http,
        request.app.state.auditoria,
        settings,
    )


@router.api_route("/api/{recurso}", methods=_METODOS, include_in_schema=False)
async def proxy_recurso(request: Request, recurso: str) -> Response:
    return await _proxy(request, recurso)


@router.api_route("/api/{recurso}/{resto:path}", methods=_METODOS, include_in_schema=False)
async def proxy_subruta(request: Request, recurso: str, resto: str) -> Response:
    return await _proxy(request, recurso)
