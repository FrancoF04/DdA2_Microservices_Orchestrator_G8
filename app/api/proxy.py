"""`/api/{recurso}/**`: puerta de entrada única hacia los microservicios."""

from __future__ import annotations

from fastapi import APIRouter, Request
from starlette.responses import Response

from app.errores import problem_response
from app.proxy.reenvio import reenviar
from app.proxy.rutas import RECURSOS, resolver_destino
from app.seguridad.autenticador import Autenticador

router = APIRouter(tags=["Proxy"])

_METODOS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


def _es_logout(recurso: str, metodo: str, ruta: str) -> bool:
    return recurso == "auth" and metodo == "POST" and ruta.rstrip("/") == "/auth/logout"


async def _proxy(request: Request, recurso: str) -> Response:
    settings = request.app.state.settings
    autenticador: Autenticador = request.app.state.autenticador
    # Se reenvía la ruta sin el prefijo /api (el recurso queda: /leads/..., /vehiculos/...).
    ruta = request.url.path.removeprefix("/api")

    # Primero la autenticación: un recurso inexistente tampoco se revela a quien no se identificó.
    identidad = autenticador.autenticar(request, recurso, ruta)

    destino = resolver_destino(recurso, settings)
    if destino is None:
        return problem_response(
            404,
            "RECURSO_NO_ENCONTRADO",
            f"No hay ningún servicio para '/api/{recurso}'.",
            extra={"recursosDisponibles": sorted(RECURSOS)},
        )

    # Logout: el token se invalida en el orquestador en el acto (lo valida localmente, no
    # se enteraría solo). Va antes de reenviar para que ya no sirva aunque auth tarde en responder.
    if autenticador.habilitada and _es_logout(recurso, request.method.upper(), ruta):
        operador = autenticador.operador_opcional(request)
        if operador is not None and operador.jti and operador.expira is not None:
            await autenticador.revocaciones.revocar(operador.jti, operador.expira)

    return await reenviar(
        request,
        destino,
        ruta,
        request.app.state.http,
        request.app.state.auditoria,
        settings,
        identidad,
    )


@router.api_route("/api/{recurso}", methods=_METODOS, include_in_schema=False)
async def proxy_recurso(request: Request, recurso: str) -> Response:
    return await _proxy(request, recurso)


@router.api_route("/api/{recurso}/{resto:path}", methods=_METODOS, include_in_schema=False)
async def proxy_subruta(request: Request, recurso: str, resto: str) -> Response:
    return await _proxy(request, recurso)
