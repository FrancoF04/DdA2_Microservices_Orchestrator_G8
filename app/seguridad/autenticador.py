"""Decide si un pedido puede pasar y a nombre de quién.

- Rutas públicas (ver `politica.es_publica`): pasan sin credenciales.
- Servicios (leads, calificacion, ia-resumen): se identifican con `X-Origen-Servicio` +
  `X-Clave-Servicio`, y solo pueden llamar a los recursos de `RECURSOS_POR_SERVICIO`.
- Operadores (telemarketers y admins): `Authorization: Bearer <access token>` de auth.
"""

from __future__ import annotations

import hmac
import logging
from typing import Optional

from fastapi import Request

from app.config import Settings
from app.errores import ErrorOrquestador
from app.seguridad.identidad import SERVICIO, Identidad
from app.seguridad.politica import RECURSOS_POR_SERVICIO, es_publica
from app.seguridad.revocaciones import Revocaciones
from app.seguridad.tokens import ALGORITMOS_PERMITIDOS, validar_access_token

logger = logging.getLogger(__name__)

HEADER_SERVICIO = "X-Origen-Servicio"
HEADER_CLAVE_SERVICIO = "X-Clave-Servicio"
HEADER_USUARIO = "X-Usuario"
HEADER_USUARIO_ROL = "X-Usuario-Rol"

MIN_LARGO_CLAVE = 32


def validar_configuracion_auth(settings: Settings) -> None:
    """Falla al arrancar si la autenticación está habilitada pero mal configurada."""
    if not settings.auth_habilitada:
        logger.warning("AUTH_HABILITADA=false: el orquestador NO exige autenticación (solo desarrollo).")
        return
    if not settings.jwt_secret:
        raise RuntimeError(
            "Falta JWT_SECRET: tiene que ser el mismo que usa AuthMicroservice (32+ caracteres). "
            "Para trabajar sin autenticación (solo desarrollo) usá AUTH_HABILITADA=false."
        )
    if len(settings.jwt_secret) < MIN_LARGO_CLAVE:
        raise RuntimeError(f"JWT_SECRET es demasiado corto: se necesitan {MIN_LARGO_CLAVE}+ caracteres.")
    if settings.jwt_algorithm not in ALGORITMOS_PERMITIDOS:
        raise RuntimeError(
            f"JWT_ALGORITHM '{settings.jwt_algorithm}' no está permitido: usar {sorted(ALGORITMOS_PERMITIDOS)}."
        )
    try:
        claves = settings.claves_servicio_por_nombre
    except ValueError as exc:
        raise RuntimeError(str(exc)) from None
    desconocidos = sorted(set(claves) - set(RECURSOS_POR_SERVICIO))
    if desconocidos:
        raise RuntimeError(
            f"CLAVES_SERVICIO tiene servicios desconocidos {desconocidos}: "
            f"los válidos son {sorted(RECURSOS_POR_SERVICIO)}."
        )
    cortas = sorted(n for n, c in claves.items() if len(c) < MIN_LARGO_CLAVE)
    if cortas:
        raise RuntimeError(f"Las claves de servicio de {cortas} tienen menos de {MIN_LARGO_CLAVE} caracteres.")
    if len(set(claves.values())) != len(claves):
        raise RuntimeError("Cada servicio tiene que tener una clave distinta en CLAVES_SERVICIO.")
    faltan = sorted(set(RECURSOS_POR_SERVICIO) - set(claves))
    if faltan:
        logger.warning(
            "CLAVES_SERVICIO no define %s: esos servicios van a recibir 401 al llamar al orquestador.", faltan
        )


class Autenticador:
    def __init__(self, settings: Settings, revocaciones: Revocaciones) -> None:
        self.settings = settings
        self.revocaciones = revocaciones
        try:
            self._claves = settings.claves_servicio_por_nombre
        except ValueError:
            self._claves = {}  # lo informa validar_configuracion_auth al arrancar

    @property
    def habilitada(self) -> bool:
        return self.settings.auth_habilitada

    def autenticar(self, request: Request, recurso: str, ruta: str) -> Optional[Identidad]:
        """Para el proxy. `None` si la ruta es pública (o la autenticación está apagada)."""
        if not self.habilitada:
            return None
        if es_publica(request.method, recurso, ruta, request.headers):
            return None
        if HEADER_CLAVE_SERVICIO in request.headers:
            return self._servicio(request, recurso)
        return self._operador(request)

    def requerir_operador(self, request: Request) -> Optional[Identidad]:
        """Para los endpoints propios (`/api/orquestador/**`): solo operadores logueados."""
        if not self.habilitada:
            return None
        return self._operador(request)

    def operador_opcional(self, request: Request) -> Optional[Identidad]:
        """El operador del pedido si trae un token válido y vigente; si no, `None` (sin error)."""
        try:
            return self._operador(request)
        except ErrorOrquestador:
            return None

    def _operador(self, request: Request) -> Identidad:
        esquema, _, token = request.headers.get("authorization", "").partition(" ")
        if esquema.lower() != "bearer" or not token.strip():
            raise ErrorOrquestador(
                401, "NO_AUTENTICADO", "Falta el token. Iniciá sesión en /api/auth/login."
            )
        identidad = validar_access_token(token.strip(), self.settings)
        if identidad.jti and self.revocaciones.esta_revocado(identidad.jti):
            raise ErrorOrquestador(401, "TOKEN_REVOCADO", "La sesión fue cerrada. Iniciá sesión de nuevo.")
        return identidad

    def _servicio(self, request: Request, recurso: str) -> Identidad:
        nombre = request.headers.get(HEADER_SERVICIO, "").strip().lower()
        recibida = request.headers.get(HEADER_CLAVE_SERVICIO, "")
        esperada = self._claves.get(nombre, "")
        # Comparación en tiempo constante; sin clave configurada nunca coincide.
        if not esperada or not hmac.compare_digest(recibida.encode(), esperada.encode()):
            raise ErrorOrquestador(
                401, "CLAVE_SERVICIO_INVALIDA", "La credencial del servicio no es válida."
            )
        if recurso not in RECURSOS_POR_SERVICIO.get(nombre, frozenset()):
            raise ErrorOrquestador(
                403, "SERVICIO_NO_AUTORIZADO", f"El servicio '{nombre}' no puede llamar a '/api/{recurso}'."
            )
        return Identidad(tipo=SERVICIO, servicio=nombre)
