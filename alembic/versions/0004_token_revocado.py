"""Tokens de acceso revocados por logout (autenticación, docs/06-autenticacion.md).

Revision ID: 0004
Revises: 0003
Create Date: 2026-11-15
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

import sqlalchemy as sa
from alembic import op

from app.db.models import SCHEMA

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "token_revocado",
        sa.Column("jti", sa.Text(), nullable=False),
        sa.Column("expira_en", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "revocado_en", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.PrimaryKeyConstraint("jti", name="pk_token_revocado"),
        schema=SCHEMA,
    )
    op.create_index("ix_token_revocado_expira_en", "token_revocado", ["expira_en"], schema=SCHEMA)


def downgrade() -> None:
    op.drop_index("ix_token_revocado_expira_en", table_name="token_revocado", schema=SCHEMA)
    op.drop_table("token_revocado", schema=SCHEMA)
