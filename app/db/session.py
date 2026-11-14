"""Conexión asincrónica a PostgreSQL (Supabase).

Pool chico, como leads: el session pooler de Supabase limita a 15 clientes
para todo el equipo.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.config import Settings

logger = logging.getLogger(__name__)

_engine: Optional[AsyncEngine] = None
_sessionmaker: Optional[async_sessionmaker[AsyncSession]] = None


def normalizar_url(url: str) -> str:
    """Fuerza el driver async de psycopg 3 (`postgresql+psycopg://`)."""
    for prefijo in ("postgresql://", "postgres://"):
        if url.startswith(prefijo):
            return "postgresql+psycopg://" + url[len(prefijo):]
    return url


def iniciar_db(settings: Settings) -> Optional[AsyncEngine]:
    global _engine, _sessionmaker
    if not settings.database_url:
        logger.warning("DATABASE_URL no configurada: el orquestador corre sin base de datos.")
        return None

    _engine = create_async_engine(
        normalizar_url(settings.database_url),
        pool_size=settings.db_pool_size,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=180,
        pool_timeout=10,
    )
    _sessionmaker = async_sessionmaker(_engine, expire_on_commit=False)
    return _engine


async def cerrar_db() -> None:
    global _engine, _sessionmaker
    if _engine is not None:
        await _engine.dispose()
    _engine = None
    _sessionmaker = None


def db_configurada() -> bool:
    return _engine is not None


async def _select_1(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def verificar_db(timeout_segundos: float = 2.0) -> bool:
    """Chequeo acotado en tiempo, para que /health no se cuelgue si la base no responde."""
    if _engine is None:
        return False
    try:
        await asyncio.wait_for(_select_1(_engine), timeout=timeout_segundos)
        return True
    except Exception as exc:  # noqa: BLE001 - el health solo informa
        logger.warning("Base de datos no disponible: %s", exc)
        return False


async def get_session() -> AsyncIterator[AsyncSession]:
    """Dependencia de FastAPI."""
    if _sessionmaker is None:
        raise RuntimeError("Base de datos no configurada.")
    async with _sessionmaker() as session:
        yield session
