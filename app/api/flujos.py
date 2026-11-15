"""`/api/orquestador/flujos`: en qué paso está cada flujo y cuánto tardó cada servicio."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.db.models import Flujo
from app.db.session import obtener_sessionmaker
from app.errores import ErrorOrquestador
from app.flujos import registro_lead as rl
from app.flujos.despachador import EVENTO_RESUMEN_SOLICITADO, ComandoSaliente, encolar
from app.seguridad.dependencias import requiere_operador

router = APIRouter(
    prefix="/api/orquestador/flujos", tags=["Orquestador"], dependencies=[Depends(requiere_operador)]
)


class PasoResponse(BaseModel):
    paso: str
    servicio: str
    canal: str
    estado: str
    iniciadoEn: datetime
    respondidoEn: Optional[datetime]
    latenciaMs: Optional[int]
    detalleError: Optional[str]


class FlujoResponse(BaseModel):
    id: uuid.UUID
    tipo: str
    claveNegocio: str
    correlationId: Optional[str]
    estado: str
    pasoActual: Optional[str]
    ultimoError: Optional[str]
    venceEn: Optional[datetime]
    iniciadoEn: datetime
    actualizadoEn: datetime
    finalizadoEn: Optional[datetime]
    pasos: list[PasoResponse]


def _a_respuesta(f: Flujo) -> FlujoResponse:
    return FlujoResponse(
        id=f.id,
        tipo=f.tipo,
        claveNegocio=f.clave_negocio,
        correlationId=f.correlation_id,
        estado=f.estado,
        pasoActual=f.paso_actual,
        ultimoError=f.ultimo_error,
        venceEn=f.vence_en,
        iniciadoEn=f.iniciado_en,
        actualizadoEn=f.actualizado_en,
        finalizadoEn=f.finalizado_en,
        pasos=[
            PasoResponse(
                paso=p.paso,
                servicio=p.servicio,
                canal=p.canal,
                estado=p.estado,
                iniciadoEn=p.iniciado_en,
                respondidoEn=p.respondido_en,
                latenciaMs=p.latencia_ms,
                detalleError=p.detalle_error,
            )
            for p in f.pasos
        ],
    )


def _sessionmaker():
    sm = obtener_sessionmaker()
    if sm is None:
        raise ErrorOrquestador(503, "BASE_NO_CONFIGURADA", "El orquestador no tiene base de datos configurada.")
    return sm


@router.get("", response_model=list[FlujoResponse], summary="Buscar flujos")
async def buscar_flujos(
    leadId: Optional[str] = Query(None, description="Lead del flujo (clave de negocio)."),
    estado: Optional[str] = Query(None, description="Ej.: EXPIRADO para ver los flujos trabados."),
    tipo: Optional[str] = Query(None, description="Ej.: REGISTRO_LEAD."),
    limite: int = Query(50, ge=1, le=500),
) -> list[FlujoResponse]:
    consulta = select(Flujo).options(selectinload(Flujo.pasos)).order_by(Flujo.iniciado_en.desc()).limit(limite)
    if leadId:
        consulta = consulta.where(Flujo.clave_negocio == leadId)
    if estado:
        consulta = consulta.where(Flujo.estado == estado.upper())
    if tipo:
        consulta = consulta.where(Flujo.tipo == tipo.upper())
    async with _sessionmaker()() as session:
        flujos = (await session.execute(consulta)).scalars().all()
    return [_a_respuesta(f) for f in flujos]


@router.get("/{flujo_id}", response_model=FlujoResponse, summary="Un flujo con todos sus pasos")
async def obtener_flujo(flujo_id: uuid.UUID) -> FlujoResponse:
    async with _sessionmaker()() as session:
        flujo = (
            await session.execute(
                select(Flujo).where(Flujo.id == flujo_id).options(selectinload(Flujo.pasos))
            )
        ).scalar_one_or_none()
    if flujo is None:
        raise ErrorOrquestador(404, "FLUJO_NO_ENCONTRADO", "No existe un flujo con ese id.")
    return _a_respuesta(flujo)


@router.post(
    "/{flujo_id}/reintentar",
    response_model=FlujoResponse,
    status_code=202,
    summary="Volver a pedir el resumen de un lead",
    description=(
        "Para un flujo `REGISTRO_LEAD` ya calificado que no terminó (EXPIRADO, FALLIDO o esperando el "
        "resumen): deja el flujo en CALIFICADO y encola de nuevo `resumen-solicitado`. Lo publica el "
        "despachador en unos segundos. Es idempotente: leads guarda el último resumen que le llega."
    ),
)
async def reintentar_flujo(flujo_id: uuid.UUID, request: Request) -> FlujoResponse:
    settings = request.app.state.settings
    if not settings.orquestador_publica_resumen:
        raise ErrorOrquestador(
            409,
            "DESPACHADOR_APAGADO",
            "El orquestador no está publicando resumen-solicitado (ORQUESTADOR_PUBLICA_RESUMEN=false).",
        )
    async with _sessionmaker()() as session, session.begin():
        flujo = (
            await session.execute(
                select(Flujo).where(Flujo.id == flujo_id).options(selectinload(Flujo.pasos)).with_for_update()
            )
        ).scalar_one_or_none()
        if flujo is None:
            raise ErrorOrquestador(404, "FLUJO_NO_ENCONTRADO", "No existe un flujo con ese id.")
        if flujo.tipo != rl.TIPO:
            raise ErrorOrquestador(409, "FLUJO_NO_REINTENTABLE", f"Los flujos {flujo.tipo} no se reintentan.")
        try:
            rl.preparar_reintento(flujo, datetime.now(timezone.utc), settings)
        except rl.ReintentoNoPermitido as exc:
            raise ErrorOrquestador(409, exc.codigo, exc.mensaje) from None
        await encolar(session, flujo, ComandoSaliente(settings.kafka_topico_resumen_solicitado, EVENTO_RESUMEN_SOLICITADO))
        await session.flush()
        # `actualizado_en` lo recalcula la base al actualizar: se vuelve a leer antes de armar la respuesta.
        await session.refresh(flujo, attribute_names=["actualizado_en"])
        return _a_respuesta(flujo)
