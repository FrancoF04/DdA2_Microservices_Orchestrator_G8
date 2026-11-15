"""`/api/orquestador/servicios`: cómo vienen respondiendo los servicios.

Se calcula sobre las llamadas reales que pasaron por el proxy (observación
pasiva). Complementa al monitor, que sondea activamente aunque no haya tráfico.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Query, Request
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, select

from app.db.models import LlamadaServicio
from app.db.session import obtener_sessionmaker
from app.errores import ErrorOrquestador
from app.proxy.rutas import servicios_configurados
from app.seguridad.dependencias import requiere_operador

router = APIRouter(
    prefix="/api/orquestador", tags=["Orquestador"], dependencies=[Depends(requiere_operador)]
)


class EstadisticaServicio(BaseModel):
    servicio: str
    configurado: bool
    llamadas: int
    errores: int
    porcentajeError: float
    latenciaP50Ms: Optional[float]
    latenciaP95Ms: Optional[float]
    ultimaLlamada: Optional[datetime]
    ultimoError: Optional[dict[str, Any]]


class EstadisticasResponse(BaseModel):
    ventanaMinutos: int
    desde: datetime
    servicios: list[EstadisticaServicio]


# Error = el orquestador no pudo hablar con el servicio, o el servicio devolvió 5xx.
_ES_ERROR = or_(LlamadaServicio.error.is_not(None), LlamadaServicio.codigo_http >= 500)


@router.get(
    "/servicios",
    response_model=EstadisticasResponse,
    summary="Llamadas, errores y latencia por servicio",
)
async def estadisticas_servicios(
    request: Request,
    ventana_minutos: int = Query(60, ge=1, le=60 * 24 * 7, description="Ventana de tiempo hacia atrás."),
) -> EstadisticasResponse:
    sessionmaker = obtener_sessionmaker()
    if sessionmaker is None:
        raise ErrorOrquestador(503, "BASE_NO_CONFIGURADA", "El orquestador no tiene base de datos configurada.")

    desde = datetime.now(timezone.utc) - timedelta(minutes=ventana_minutos)
    en_ventana = LlamadaServicio.ocurrida_en >= desde

    agregados = (
        select(
            LlamadaServicio.destino,
            func.count().label("llamadas"),
            func.count().filter(_ES_ERROR).label("errores"),
            func.percentile_cont(0.5).within_group(LlamadaServicio.latencia_ms).label("p50"),
            func.percentile_cont(0.95).within_group(LlamadaServicio.latencia_ms).label("p95"),
            func.max(LlamadaServicio.ocurrida_en).label("ultima"),
        )
        .where(en_ventana)
        .group_by(LlamadaServicio.destino)
    )
    ultimos_errores = (
        select(
            LlamadaServicio.destino,
            LlamadaServicio.metodo,
            LlamadaServicio.ruta,
            LlamadaServicio.codigo_http,
            LlamadaServicio.error,
            LlamadaServicio.ocurrida_en,
        )
        .where(and_(en_ventana, _ES_ERROR))
        .distinct(LlamadaServicio.destino)
        .order_by(LlamadaServicio.destino, LlamadaServicio.ocurrida_en.desc())
    )

    async with sessionmaker() as session:
        filas = {f.destino: f for f in (await session.execute(agregados)).all()}
        errores = {f.destino: f for f in (await session.execute(ultimos_errores)).all()}

    configurados = servicios_configurados(request.app.state.settings)
    nombres = list(configurados) + sorted(set(filas) - set(configurados))

    servicios = []
    for nombre in nombres:
        fila = filas.get(nombre)
        err = errores.get(nombre)
        llamadas = fila.llamadas if fila else 0
        cant_errores = fila.errores if fila else 0
        servicios.append(
            EstadisticaServicio(
                servicio=nombre,
                configurado=bool(configurados.get(nombre)),
                llamadas=llamadas,
                errores=cant_errores,
                porcentajeError=round(100 * cant_errores / llamadas, 1) if llamadas else 0.0,
                latenciaP50Ms=round(fila.p50, 1) if fila and fila.p50 is not None else None,
                latenciaP95Ms=round(fila.p95, 1) if fila and fila.p95 is not None else None,
                ultimaLlamada=fila.ultima if fila else None,
                ultimoError=(
                    {
                        "metodo": err.metodo,
                        "ruta": err.ruta,
                        "codigoHttp": err.codigo_http,
                        "error": err.error,
                        "ocurridaEn": err.ocurrida_en,
                    }
                    if err
                    else None
                ),
            )
        )

    return EstadisticasResponse(ventanaMinutos=ventana_minutos, desde=desde, servicios=servicios)
