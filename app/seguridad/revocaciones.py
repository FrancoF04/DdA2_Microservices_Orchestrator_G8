"""Access tokens invalidados por logout.

Como el orquestador valida los JWT localmente, no se entera solo de que un operador
cerró sesión. El logout pasa por acá (`POST /api/auth/logout`), así que se anota el `jti`
del token hasta que venza. Se consulta en memoria (una consulta a la base por pedido sería
caro) y se persiste para que un reinicio no "resucite" tokens cerrados.

Limitación: con varias réplicas, cada una conoce los logouts que pasaron por ella hasta
reiniciar (los tokens duran 15 minutos). Para un TP con una réplica alcanza.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from app.db import session as db
from app.db.models import TokenRevocado

logger = logging.getLogger(__name__)


class Revocaciones:
    def __init__(self) -> None:
        self._expiraciones: dict[str, datetime] = {}

    def esta_revocado(self, jti: str) -> bool:
        expira = self._expiraciones.get(jti)
        if expira is None:
            return False
        if expira <= datetime.now(timezone.utc):
            # Ya venció solo: el token deja de servir por su propio `exp`.
            del self._expiraciones[jti]
            return False
        return True

    async def revocar(self, jti: str, expira: datetime) -> None:
        self._expiraciones[jti] = expira
        sessionmaker = db.obtener_sessionmaker()
        if sessionmaker is None:
            return
        try:
            async with sessionmaker() as s:
                await s.execute(
                    insert(TokenRevocado)
                    .values(jti=jti, expira_en=expira)
                    .on_conflict_do_nothing(index_elements=[TokenRevocado.jti])
                )
                await s.commit()
        except Exception:  # noqa: BLE001 - igual queda revocado en memoria
            logger.exception("No se pudo guardar el token revocado en la base.")

    async def cargar(self) -> int:
        """Al arrancar: recupera los revocados vigentes y limpia los vencidos."""
        sessionmaker = db.obtener_sessionmaker()
        if sessionmaker is None:
            return 0
        ahora = datetime.now(timezone.utc)
        try:
            async with sessionmaker() as s:
                await s.execute(delete(TokenRevocado).where(TokenRevocado.expira_en <= ahora))
                filas = (await s.execute(select(TokenRevocado.jti, TokenRevocado.expira_en))).all()
                await s.commit()
        except Exception:  # noqa: BLE001 - p. ej. falta la migración 0004
            logger.exception("No se pudieron cargar los tokens revocados.")
            return 0
        for jti, expira in filas:
            self._expiraciones[jti] = expira
        return len(filas)
