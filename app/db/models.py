"""Modelo de datos del orquestador (schema `orquestador`).

Solo estado de flujos, pasos y tiempos. Nunca datos personales ni cuerpos de
request/response (ver docs/02-propuesta-orquestador.md §6.3).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, DateTime, ForeignKey, Integer, MetaData, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from app.config import get_settings

SCHEMA = get_settings().db_schema


class Base(DeclarativeBase):
    # La convención de índices tiene que coincidir con los nombres que crean las
    # migraciones (ix_<tabla>_<columna>), si no `alembic check` detecta diferencias.
    metadata = MetaData(
        schema=SCHEMA,
        naming_convention={"ix": "ix_%(table_name)s_%(column_0_name)s"},
    )


class Flujo(Base):
    """Instancia de un flujo de negocio de varios pasos (ej. REGISTRO_LEAD).

    En la teoría de microservicios es el patrón *Saga*; en el código y la base
    se llama `flujo` por claridad (ver docs/02-propuesta-orquestador.md §6.2).
    """

    __tablename__ = "flujo"
    __table_args__ = (UniqueConstraint("tipo", "clave_negocio", name="uq_flujo_tipo_clave_negocio"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    clave_negocio: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    correlation_id: Mapped[Optional[str]] = mapped_column(Text, index=True)
    estado: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    paso_actual: Mapped[Optional[str]] = mapped_column(Text)
    intentos: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    ultimo_error: Mapped[Optional[str]] = mapped_column(Text)
    vence_en: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    iniciado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    actualizado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    finalizado_en: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    pasos: Mapped[list["FlujoPaso"]] = relationship(
        back_populates="flujo", cascade="all, delete-orphan", order_by="FlujoPaso.id"
    )

    __mapper_args__ = {"version_id_col": version}


class FlujoPaso(Base):
    """Un paso de un flujo: interacción con un servicio y su tiempo de respuesta."""

    __tablename__ = "flujo_paso"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    flujo_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("flujo.id", ondelete="CASCADE"), nullable=False, index=True
    )
    paso: Mapped[str] = mapped_column(Text, nullable=False)
    servicio: Mapped[str] = mapped_column(Text, nullable=False)
    canal: Mapped[str] = mapped_column(Text, nullable=False)  # REST | KAFKA
    estado: Mapped[str] = mapped_column(Text, nullable=False)  # PENDIENTE | OK | ERROR | TIMEOUT
    intento: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    iniciado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    respondido_en: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    latencia_ms: Mapped[Optional[int]] = mapped_column(Integer)
    codigo_http: Mapped[Optional[int]] = mapped_column(Integer)
    referencia: Mapped[Optional[str]] = mapped_column(Text)
    detalle_error: Mapped[Optional[str]] = mapped_column(Text)

    flujo: Mapped[Flujo] = relationship(back_populates="pasos")


class LlamadaServicio(Base):
    """Auditoría de cada llamada HTTP que pasa por el proxy (sin cuerpos)."""

    __tablename__ = "llamada_servicio"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    correlation_id: Mapped[Optional[str]] = mapped_column(Text, index=True)
    origen: Mapped[str] = mapped_column(Text, nullable=False)
    destino: Mapped[str] = mapped_column(Text, nullable=False, index=True)
    metodo: Mapped[str] = mapped_column(Text, nullable=False)
    ruta: Mapped[str] = mapped_column(Text, nullable=False)
    codigo_http: Mapped[Optional[int]] = mapped_column(Integer)
    latencia_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    error: Mapped[Optional[str]] = mapped_column(Text)
    ocurrida_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )


class EventoProcesado(Base):
    """Idempotencia del consumo de Kafka: eventId o topic:partition:offset."""

    __tablename__ = "evento_procesado"

    clave: Mapped[str] = mapped_column(Text, primary_key=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    procesado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TokenRevocado(Base):
    """Access token de un operador invalidado por logout, identificado por su `jti`.

    El orquestador valida los JWT localmente, así que necesita saber cuáles se
    cerraron. Se guarda solo el `jti` hasta que el token vence (nunca el token).
    """

    __tablename__ = "token_revocado"

    jti: Mapped[str] = mapped_column(Text, primary_key=True)
    expira_en: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    revocado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Outbox(Base):
    """Comandos pendientes de publicar en Kafka (publicación confiable).

    `mensaje` guarda solo a qué lead se refiere el comando; los datos del payload se
    consultan a leads al publicar, así no se copian datos de negocio acá.
    Un comando pendiente tiene `publicado_en` y `abandonado_en` en NULL.
    """

    __tablename__ = "outbox"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    flujo_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("flujo.id", ondelete="CASCADE"), index=True
    )
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    clave: Mapped[str] = mapped_column(Text, nullable=False)
    mensaje: Mapped[dict] = mapped_column(JSONB, nullable=False)
    creado_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    intentos: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    ultimo_error: Mapped[Optional[str]] = mapped_column(Text)
    proximo_intento_en: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    publicado_en: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    abandonado_en: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
