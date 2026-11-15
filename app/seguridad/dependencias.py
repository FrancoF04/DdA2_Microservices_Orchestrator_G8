"""Dependencia de FastAPI para los endpoints propios del orquestador."""

from __future__ import annotations

from typing import Optional

from fastapi import Request

from app.seguridad.identidad import Identidad


async def requiere_operador(request: Request) -> Optional[Identidad]:
    """Exige un operador logueado (JWT válido). Con la autenticación apagada, deja pasar."""
    return request.app.state.autenticador.requerir_operador(request)
