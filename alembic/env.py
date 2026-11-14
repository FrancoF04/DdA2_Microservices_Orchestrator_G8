"""Entorno de Alembic (async). La URL se toma de DATABASE_URL."""

from __future__ import annotations

import asyncio
import sys
from logging.config import fileConfig

from sqlalchemy import pool, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from alembic import context
from app.config import get_settings
from app.db.models import SCHEMA, Base
from app.db.session import normalizar_url

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _solo_schema_propio(nombre, tipo, nombre_padre) -> bool:
    """Limita la comparación (autogenerate / `alembic check`) al schema del orquestador.

    La base es compartida (un schema por servicio, como en Supabase) y el rol del
    orquestador no tiene permisos sobre los schemas de los demás.
    """
    if tipo == "schema":
        return nombre == SCHEMA
    return True


def _url() -> str:
    url = get_settings().database_url
    if not url:
        raise RuntimeError("DATABASE_URL no está configurada; no se pueden correr migraciones.")
    return normalizar_url(url)


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        include_schemas=True,
        include_name=_solo_schema_propio,
        version_table_schema=SCHEMA,
    )
    with context.begin_transaction():
        context.run_migrations()


def _correr(connection: Connection) -> None:
    # Se consulta antes de crear: `CREATE SCHEMA IF NOT EXISTS` exige permiso de
    # creación sobre la base aunque el schema ya exista, y un rol restringido
    # (orquestador_app en Supabase, orquestador en local) no lo tiene.
    existe = connection.execute(
        text("SELECT 1 FROM pg_namespace WHERE nspname = :s"), {"s": SCHEMA}
    ).scalar()
    if not existe:
        connection.execute(text(f'CREATE SCHEMA "{SCHEMA}"'))
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        include_schemas=True,
        include_name=_solo_schema_propio,
        version_table_schema=SCHEMA,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    # search_path=public para que el schema del orquestador NO sea el "por defecto"
    # de esta conexión: si lo es, Alembic lo trata como schema anónimo y no lo
    # empareja con los modelos (que lo declaran explícito). Las migraciones
    # indican el schema en cada operación, así que no dependen del search_path.
    engine = create_async_engine(
        _url(), poolclass=pool.NullPool, connect_args={"options": "-c search_path=public"}
    )
    async with engine.connect() as connection:
        await connection.run_sync(_correr)
        await connection.commit()
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    # En Windows el loop por defecto (Proactor) no sirve para psycopg async.
    asyncio.run(
        run_migrations_online(),
        loop_factory=asyncio.SelectorEventLoop if sys.platform == "win32" else None,
    )
