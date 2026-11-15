"""Quién hace un pedido autenticado: un operador (JWT) o un servicio (credencial)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

OPERADOR = "operador"
SERVICIO = "servicio"


@dataclass(frozen=True)
class Identidad:
    tipo: str
    # Solo operadores (salen de los claims del JWT):
    usuario_id: Optional[str] = None
    email: Optional[str] = None
    rol: Optional[str] = None
    jti: Optional[str] = None
    expira: Optional[datetime] = None
    # Solo servicios:
    servicio: Optional[str] = None

    @property
    def usuario(self) -> Optional[str]:
        """Valor para el header `X-Usuario`: el email, o el id si el token no lo trae."""
        return self.email or self.usuario_id
