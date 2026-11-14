"""Renombra saga → flujo y saga_paso → flujo_paso (y sus columnas, índices y constraints).

"Saga" es el nombre del patrón en la teoría de microservicios; para el equipo
resulta más claro "flujo". Como "flujo" es masculino, las columnas de fecha
pasan a iniciado_en / actualizado_en / finalizado_en. Las tablas estaban vacías
al momento del cambio.

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-14
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Union

from alembic import op

from app.db.models import SCHEMA

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Índices: (nombre viejo, nombre nuevo). Constraints: (tabla, nombre viejo, nombre nuevo).
_INDICES = [
    ("ix_saga_clave_negocio", "ix_flujo_clave_negocio"),
    ("ix_saga_correlation_id", "ix_flujo_correlation_id"),
    ("ix_saga_estado", "ix_flujo_estado"),
    ("ix_saga_vence_en", "ix_flujo_vence_en"),
    ("ix_saga_paso_saga_id", "ix_flujo_paso_flujo_id"),
]
_CONSTRAINTS = [
    ("flujo", "saga_pkey", "flujo_pkey"),
    ("flujo_paso", "saga_paso_pkey", "flujo_paso_pkey"),
    ("flujo_paso", "saga_paso_saga_id_fkey", "flujo_paso_flujo_id_fkey"),
]
# Columnas: (tabla, nombre viejo, nombre nuevo).
_COLUMNAS = [
    ("flujo_paso", "saga_id", "flujo_id"),
    ("flujo", "iniciada_en", "iniciado_en"),
    ("flujo", "actualizada_en", "actualizado_en"),
    ("flujo", "finalizada_en", "finalizado_en"),
]


def _q(nombre: str) -> str:
    return f'"{SCHEMA}"."{nombre}"'


def upgrade() -> None:
    op.rename_table("saga", "flujo", schema=SCHEMA)
    op.rename_table("saga_paso", "flujo_paso", schema=SCHEMA)
    for tabla, viejo, nuevo in _COLUMNAS:
        op.alter_column(tabla, viejo, new_column_name=nuevo, schema=SCHEMA)
    for viejo, nuevo in _INDICES:
        op.execute(f'ALTER INDEX {_q(viejo)} RENAME TO "{nuevo}"')
    for tabla, viejo, nuevo in _CONSTRAINTS:
        op.execute(f'ALTER TABLE {_q(tabla)} RENAME CONSTRAINT "{viejo}" TO "{nuevo}"')
    # La secuencia del id autoincremental no se renombra sola con la tabla.
    op.execute(f'ALTER SEQUENCE {_q("saga_paso_id_seq")} RENAME TO "flujo_paso_id_seq"')


def downgrade() -> None:
    op.execute(f'ALTER SEQUENCE {_q("flujo_paso_id_seq")} RENAME TO "saga_paso_id_seq"')
    for tabla, viejo, nuevo in _CONSTRAINTS:
        op.execute(f'ALTER TABLE {_q(tabla)} RENAME CONSTRAINT "{nuevo}" TO "{viejo}"')
    for viejo, nuevo in _INDICES:
        op.execute(f'ALTER INDEX {_q(nuevo)} RENAME TO "{viejo}"')
    for tabla, viejo, nuevo in _COLUMNAS:
        op.alter_column(tabla, nuevo, new_column_name=viejo, schema=SCHEMA)
    op.rename_table("flujo_paso", "saga_paso", schema=SCHEMA)
    op.rename_table("flujo", "saga", schema=SCHEMA)
