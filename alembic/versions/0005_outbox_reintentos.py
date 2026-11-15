"""Outbox con reintentos: flujo, intentos, error, próximo intento y abandono (tanda A).

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-15
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from app.db.models import SCHEMA

revision: str = "0005"
down_revision: Union[str, None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("outbox", sa.Column("flujo_id", postgresql.UUID(as_uuid=True), nullable=True), schema=SCHEMA)
    op.add_column("outbox", sa.Column("intentos", sa.Integer(), server_default="0", nullable=False), schema=SCHEMA)
    op.add_column("outbox", sa.Column("ultimo_error", sa.Text(), nullable=True), schema=SCHEMA)
    op.add_column(
        "outbox",
        sa.Column("proximo_intento_en", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        schema=SCHEMA,
    )
    op.add_column("outbox", sa.Column("abandonado_en", sa.DateTime(timezone=True), nullable=True), schema=SCHEMA)
    op.create_foreign_key(
        "fk_outbox_flujo_id_flujo",
        "outbox",
        "flujo",
        ["flujo_id"],
        ["id"],
        source_schema=SCHEMA,
        referent_schema=SCHEMA,
        ondelete="CASCADE",
    )
    op.create_index("ix_outbox_flujo_id", "outbox", ["flujo_id"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_outbox_flujo_id", table_name="outbox", schema=SCHEMA)
    op.drop_constraint("fk_outbox_flujo_id_flujo", "outbox", schema=SCHEMA, type_="foreignkey")
    for columna in ("abandonado_en", "proximo_intento_en", "ultimo_error", "intentos", "flujo_id"):
        op.drop_column("outbox", columna, schema=SCHEMA)
