"""Tablas iniciales: saga, saga_paso, llamada_servicio, evento_procesado, outbox.

Revision ID: 0001
Revises:
Create Date: 2026-11-14
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.db.models import SCHEMA

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_TS = sa.DateTime(timezone=True)


def upgrade() -> None:
    op.create_table(
        "saga",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("tipo", sa.Text(), nullable=False),
        sa.Column("clave_negocio", sa.Text(), nullable=False),
        sa.Column("correlation_id", sa.Text()),
        sa.Column("estado", sa.Text(), nullable=False),
        sa.Column("paso_actual", sa.Text()),
        sa.Column("intentos", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("ultimo_error", sa.Text()),
        sa.Column("vence_en", _TS),
        sa.Column("iniciada_en", _TS, nullable=False, server_default=sa.func.now()),
        sa.Column("actualizada_en", _TS, nullable=False, server_default=sa.func.now()),
        sa.Column("finalizada_en", _TS),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        schema=SCHEMA,
    )
    op.create_index("ix_saga_clave_negocio", "saga", ["clave_negocio"], schema=SCHEMA)
    op.create_index("ix_saga_correlation_id", "saga", ["correlation_id"], schema=SCHEMA)
    op.create_index("ix_saga_estado", "saga", ["estado"], schema=SCHEMA)
    op.create_index("ix_saga_vence_en", "saga", ["vence_en"], schema=SCHEMA)

    op.create_table(
        "saga_paso",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "saga_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(f"{SCHEMA}.saga.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("paso", sa.Text(), nullable=False),
        sa.Column("servicio", sa.Text(), nullable=False),
        sa.Column("canal", sa.Text(), nullable=False),
        sa.Column("estado", sa.Text(), nullable=False),
        sa.Column("intento", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("iniciado_en", _TS, nullable=False, server_default=sa.func.now()),
        sa.Column("respondido_en", _TS),
        sa.Column("latencia_ms", sa.Integer()),
        sa.Column("codigo_http", sa.Integer()),
        sa.Column("referencia", sa.Text()),
        sa.Column("detalle_error", sa.Text()),
        schema=SCHEMA,
    )
    op.create_index("ix_saga_paso_saga_id", "saga_paso", ["saga_id"], schema=SCHEMA)

    op.create_table(
        "llamada_servicio",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("correlation_id", sa.Text()),
        sa.Column("origen", sa.Text(), nullable=False),
        sa.Column("destino", sa.Text(), nullable=False),
        sa.Column("metodo", sa.Text(), nullable=False),
        sa.Column("ruta", sa.Text(), nullable=False),
        sa.Column("codigo_http", sa.Integer()),
        sa.Column("latencia_ms", sa.Integer(), nullable=False),
        sa.Column("error", sa.Text()),
        sa.Column("ocurrida_en", _TS, nullable=False, server_default=sa.func.now()),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_llamada_servicio_correlation_id", "llamada_servicio", ["correlation_id"], schema=SCHEMA
    )
    op.create_index("ix_llamada_servicio_destino", "llamada_servicio", ["destino"], schema=SCHEMA)
    op.create_index(
        "ix_llamada_servicio_ocurrida_en", "llamada_servicio", ["ocurrida_en"], schema=SCHEMA
    )

    op.create_table(
        "evento_procesado",
        sa.Column("clave", sa.Text(), primary_key=True),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("procesado_en", _TS, nullable=False, server_default=sa.func.now()),
        schema=SCHEMA,
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("clave", sa.Text(), nullable=False),
        sa.Column("mensaje", postgresql.JSONB(), nullable=False),
        sa.Column("creado_en", _TS, nullable=False, server_default=sa.func.now()),
        sa.Column("publicado_en", _TS),
        schema=SCHEMA,
    )
    op.create_index("ix_outbox_publicado_en", "outbox", ["publicado_en"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_table("outbox", schema=SCHEMA)
    op.drop_table("evento_procesado", schema=SCHEMA)
    op.drop_table("llamada_servicio", schema=SCHEMA)
    op.drop_table("saga_paso", schema=SCHEMA)
    op.drop_table("saga", schema=SCHEMA)
