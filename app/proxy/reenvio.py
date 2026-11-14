"""Reenvío de una request al servicio destino y de su respuesta al cliente.

- El body se reenvía en streaming (sirve para el multipart de fotos de vehiculos).
- La respuesta del destino se devuelve tal cual (incluidos sus 4xx/5xx), en
  streaming y sin decodificar.
- Solo los errores de red los responde el propio orquestador en ProblemDetail:
  503 (no se pudo conectar), 504 (no respondió a tiempo), 502 (cortó la conexión).
- Se quitan los headers de CORS del destino: CORS lo resuelve solo el
  orquestador (si no, el navegador recibe headers duplicados y rechaza la respuesta).
- Nunca se registran cuerpos (el POST /leads lleva el CUIL).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import Request
from starlette.background import BackgroundTask
from starlette.responses import Response, StreamingResponse

from app.auditoria import Llamada, RegistroAuditoria
from app.config import Settings
from app.errores import problem_response
from app.middleware.correlation_id import HEADER as CORRELATION_HEADER
from app.middleware.correlation_id import correlation_id_actual
from app.proxy.clientes_http import ClientesHttp
from app.proxy.rutas import Destino

logger = logging.getLogger(__name__)

HEADER_ORIGEN = "X-Origen-Servicio"

# Headers que no se reenvían (RFC 9110 §7.6.1) más los que recalcula el proxy.
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
_NO_REENVIAR_REQUEST = _HOP_BY_HOP | {"host", CORRELATION_HEADER.lower()}
_NO_REENVIAR_RESPONSE = _HOP_BY_HOP | {CORRELATION_HEADER.lower()}

_METODOS_SIN_BODY = {"GET", "HEAD", "OPTIONS"}


def detectar_origen(request: Request) -> str:
    """Quién llama. Fase 1: se infiere; fase 2: cada servicio manda X-Origen-Servicio."""
    declarado = request.headers.get(HEADER_ORIGEN)
    if declarado:
        return declarado.strip().lower()[:50]
    if request.headers.get("origin"):
        return "front"
    return "desconocido"


def _headers_request(request: Request, cid: str) -> list[tuple[str, str]]:
    headers = [
        (k, v) for k, v in request.headers.items() if k.lower() not in _NO_REENVIAR_REQUEST
    ]
    headers.append((CORRELATION_HEADER, cid))
    if "x-forwarded-for" not in request.headers and request.client:
        headers.append(("X-Forwarded-For", request.client.host))
    if "x-forwarded-proto" not in request.headers:
        headers.append(("X-Forwarded-Proto", request.url.scheme))
    if "x-forwarded-host" not in request.headers and request.headers.get("host"):
        headers.append(("X-Forwarded-Host", request.headers["host"]))
    # httpx agrega "Accept-Encoding: gzip" por defecto. Como el body se devuelve
    # sin decodificar, si el cliente no pidió compresión hay que pedirla sin comprimir.
    if "accept-encoding" not in request.headers:
        headers.append(("Accept-Encoding", "identity"))
    return headers


def _headers_response(respuesta: httpx.Response) -> list[tuple[bytes, bytes]]:
    return [
        (k.encode("latin-1"), v.encode("latin-1"))
        for k, v in respuesta.headers.multi_items()
        if k.lower() not in _NO_REENVIAR_RESPONSE and not k.lower().startswith("access-control-")
    ]


def _tiene_body(request: Request) -> bool:
    return "content-length" in request.headers or "transfer-encoding" in request.headers


async def reenviar(
    request: Request,
    destino: Destino,
    ruta: str,
    clientes: ClientesHttp,
    auditoria: RegistroAuditoria,
    settings: Settings,
) -> Response:
    cid = correlation_id_actual() or ""
    origen = detectar_origen(request)
    metodo = request.method.upper()

    def auditar(codigo: Optional[int], latencia_ms: int, error: Optional[str]) -> None:
        auditoria.registrar(
            Llamada(
                correlation_id=cid or None,
                origen=origen,
                destino=destino.servicio,
                metodo=metodo,
                ruta=ruta[:500],
                codigo_http=codigo,
                latencia_ms=latencia_ms,
                error=error,
                ocurrida_en=datetime.now(timezone.utc),
            )
        )

    if not destino.url_base:
        auditar(None, 0, "NO_CONFIGURADO")
        return problem_response(
            503,
            "SERVICIO_NO_CONFIGURADO",
            f"El servicio '{destino.servicio}' no está configurado en el orquestador.",
            extra={"servicio": destino.servicio},
        )

    url = destino.url_base.rstrip("/") + ruta
    if request.url.query:
        url += "?" + request.url.query  # tal cual, sin re-codificar
    con_body = metodo not in _METODOS_SIN_BODY and _tiene_body(request)
    headers = _headers_request(request, cid)
    timeout = httpx.Timeout(
        settings.proxy_timeout_segundos, connect=settings.proxy_timeout_conexion_segundos
    )

    # Reintentos solo sin body (el body es un stream que se consume una vez) y
    # ante fallas de conexión, donde el destino no llegó a procesar el pedido.
    intentos = 1 + (settings.proxy_reintentos_get if not con_body else 0)
    inicio = time.perf_counter()
    respuesta: Optional[httpx.Response] = None
    for intento in range(1, intentos + 1):
        http = clientes.para(destino.servicio)
        peticion = http.build_request(
            metodo,
            url,
            headers=headers,
            content=request.stream() if con_body else None,
            timeout=timeout,
        )
        try:
            respuesta = await http.send(peticion, stream=True)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.RemoteProtocolError) as exc:
            # RemoteProtocolError al reusar una conexión que el otro extremo ya cerró.
            logger.warning(
                "%s: %s (intento %s/%s): %s", destino.servicio, type(exc).__name__, intento, intentos, exc
            )
            if isinstance(exc, httpx.ConnectTimeout):
                clientes.reiniciar(destino.servicio, "ConnectTimeout")
            if intento < intentos:
                continue
            if isinstance(exc, httpx.RemoteProtocolError):
                return _error_red(502, "SERVICIO_RECHAZO", "cortó la conexión", destino, inicio, auditar)
            return _error_red(503, "SERVICIO_NO_DISPONIBLE", "no está disponible", destino, inicio, auditar)
        except httpx.TimeoutException as exc:
            logger.warning("%s: %s después de %ss", destino.servicio, type(exc).__name__, settings.proxy_timeout_segundos)
            clientes.reiniciar(destino.servicio, type(exc).__name__)
            return _error_red(504, "SERVICIO_TIMEOUT", "no respondió a tiempo", destino, inicio, auditar)
        except httpx.HTTPError as exc:
            logger.warning("%s: %s: %s", destino.servicio, type(exc).__name__, exc)
            return _error_red(502, "SERVICIO_RECHAZO", "respondió mal", destino, inicio, auditar)

    assert respuesta is not None
    latencia_ms = int((time.perf_counter() - inicio) * 1000)

    async def cerrar() -> None:
        await respuesta.aclose()
        auditar(respuesta.status_code, latencia_ms, None)

    # Si el cuerpo ya está en memoria (respuestas armadas por un transporte en
    # memoria, ej. tests), se manda entero; si no, en streaming sin decodificar.
    cuerpo = _en_memoria(respuesta.content) if respuesta.is_stream_consumed else respuesta.aiter_raw()
    salida = StreamingResponse(
        cuerpo,
        status_code=respuesta.status_code,
        background=BackgroundTask(cerrar),
    )
    salida.raw_headers = _headers_response(respuesta)
    return salida


async def _en_memoria(contenido: bytes):
    yield contenido


def _error_red(status, codigo, texto, destino: Destino, inicio: float, auditar) -> Response:
    latencia_ms = int((time.perf_counter() - inicio) * 1000)
    auditar(None, latencia_ms, codigo)
    return problem_response(
        status,
        codigo,
        f"El servicio '{destino.servicio}' {texto}.",
        extra={"servicio": destino.servicio},
    )
