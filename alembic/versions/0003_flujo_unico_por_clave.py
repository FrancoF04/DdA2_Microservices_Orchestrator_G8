"""Un solo flujo por (tipo, clave_negocio): ej. un REGISTRO_LEAD por lead.

Permite crear el flujo con INSERT ... ON CONFLICT DO NOTHING cuando llegan dos
eventos del mismo lead casi a la vez.

Revision ID: 0003
Revises: 0002
Create Date: 2026-11-14
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op

from app.db.models import SCHEMA

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_flujo_tipo_clave_negocio", "flujo", ["tipo", "clave_negocio"], schema=SCHEMA
    )


def downgrade() -> None:
    op.drop_constraint("uq_flujo_tipo_clave_negocio", "flujo", type_="unique", schema=SCHEMA)
