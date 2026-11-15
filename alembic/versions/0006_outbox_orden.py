"""Outbox con orden de publicación por lead (tanda B: varios comandos por lead).

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-15
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

from app.db.models import SCHEMA

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Las filas que ya existen reciben su número al agregar la columna.
    op.add_column("outbox", sa.Column("orden", sa.BigInteger(), sa.Identity(), nullable=False), schema=SCHEMA)
    op.create_index("ix_outbox_orden", "outbox", ["orden"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_outbox_orden", table_name="outbox", schema=SCHEMA)
    op.drop_column("outbox", "orden", schema=SCHEMA)
