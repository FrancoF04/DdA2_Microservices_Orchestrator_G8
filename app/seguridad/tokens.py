"""Validación local del access token JWT que emite AuthMicroservice.

No se llama a auth en cada pedido: se verifica la firma con el mismo secreto (HS256).
Así, si auth se cae, los operadores que ya tienen sesión siguen trabajando.
"""

from __future__ import annotations

from datetime import datetime, timezone

import jwt

from app.config import Settings
from app.errores import ErrorOrquestador
from app.seguridad.identidad import OPERADOR, Identidad

# Solo algoritmos simétricos: con un secreto compartido, nunca "none" ni asimétricos.
ALGORITMOS_PERMITIDOS = {"HS256", "HS384", "HS512"}
# Tolerancia por diferencia de reloj entre auth y el orquestador (están en hosts distintos).
_MARGEN_SEGUNDOS = 5


def validar_access_token(token: str, settings: Settings) -> Identidad:
    """Devuelve la identidad del operador o lanza `ErrorOrquestador` 401."""
    try:
        claims = jwt.decode(
            token,
            settings.jwt_secret or "",
            algorithms=[settings.jwt_algorithm],
            options={"require": ["exp", "sub", "jti"]},
            leeway=_MARGEN_SEGUNDOS,
        )
    except jwt.ExpiredSignatureError:
        raise ErrorOrquestador(
            401, "TOKEN_EXPIRADO", "El token venció. Renovalo con /api/auth/refresh."
        ) from None
    except jwt.InvalidTokenError:
        raise ErrorOrquestador(401, "TOKEN_INVALIDO", "El token no es válido.") from None

    email = claims.get("email")
    rol = claims.get("rol")
    return Identidad(
        tipo=OPERADOR,
        usuario_id=str(claims["sub"]),
        email=email if isinstance(email, str) else None,
        rol=rol if isinstance(rol, str) else None,
        jti=str(claims["jti"]),
        expira=datetime.fromtimestamp(claims["exp"], tz=timezone.utc),
    )
