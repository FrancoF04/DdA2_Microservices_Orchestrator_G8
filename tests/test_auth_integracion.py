"""Los tokens revocados sobreviven a un reinicio (contra el Postgres del compose local).

Se saltean si no hay base: definir TEST_DATABASE_URL o tener levantado `docker compose up -d`
con las migraciones aplicadas (`alembic upgrade head`).
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import delete, select, text

from app.config import Settings
from app.db import session as db
from app.db.models import TokenRevocado
from app.seguridad.revocaciones import Revocaciones

URL = os.getenv("TEST_DATABASE_URL", "postgresql://orquestador:orquestador@127.0.0.1:5432/postgres")


@pytest.fixture(scope="session")
def event_loop_policy():
    # psycopg async no funciona con el loop por defecto de Windows.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@pytest.fixture
async def base():
    db.iniciar_db(Settings(_env_file=None, database_url=URL, kafka_enabled=False))
    listo = False
    try:
        if not await db.verificar_db(timeout_segundos=2):
            pytest.skip("No hay Postgres disponible para tests de integración.")
        async with db.obtener_sessionmaker()() as s:
            existe = (await s.execute(text("select to_regclass('orquestador.token_revocado')"))).scalar()
        if existe is None:
            pytest.skip("Falta la migración 0004 (alembic upgrade head).")
        listo = True
        yield
    finally:
        # Solo se limpia si la base estaba disponible: si el test se salteó, no hay a qué conectarse.
        if listo:
            async with db.obtener_sessionmaker()() as s:
                await s.execute(delete(TokenRevocado).where(TokenRevocado.jti.like("test-%")))
                await s.commit()
        await db.cerrar_db()


async def test_un_token_revocado_se_recupera_despues_de_reiniciar(base):
    jti = f"test-{uuid.uuid4()}"
    await Revocaciones().revocar(jti, datetime.now(timezone.utc) + timedelta(minutes=15))

    reiniciado = Revocaciones()  # un proceso nuevo: no sabe nada en memoria
    assert not reiniciado.esta_revocado(jti)
    assert await reiniciado.cargar() >= 1
    assert reiniciado.esta_revocado(jti)


async def test_revocar_dos_veces_el_mismo_token_no_falla(base):
    jti = f"test-{uuid.uuid4()}"
    vence = datetime.now(timezone.utc) + timedelta(minutes=15)
    rev = Revocaciones()
    await rev.revocar(jti, vence)
    await rev.revocar(jti, vence)
    async with db.obtener_sessionmaker()() as s:
        filas = (await s.execute(select(TokenRevocado).where(TokenRevocado.jti == jti))).all()
    assert len(filas) == 1


async def test_al_cargar_se_limpian_los_vencidos(base):
    jti = f"test-{uuid.uuid4()}"
    await Revocaciones().revocar(jti, datetime.now(timezone.utc) - timedelta(minutes=1))

    reiniciado = Revocaciones()
    await reiniciado.cargar()
    assert not reiniciado.esta_revocado(jti)
    async with db.obtener_sessionmaker()() as s:
        filas = (await s.execute(select(TokenRevocado).where(TokenRevocado.jti == jti))).all()
    assert filas == []
