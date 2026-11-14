"""Auditoría de las llamadas que pasan por el proxy (tabla `llamada_servicio`).

Las llamadas se encolan en memoria y una tarea de fondo las inserta en tandas,
así una request no espera a la base ni ocupa conexiones del pool (que es chico
por el límite del pooler de Supabase). Si la cola se llena, se descartan
registros: la auditoría nunca debe frenar el tráfico.

También borra periódicamente los registros más viejos que
`auditoria_retencion_dias`.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import delete, insert

from app.config import Settings
from app.db.models import LlamadaServicio
from app.db.session import obtener_sessionmaker

logger = logging.getLogger(__name__)

TAMANIO_TANDA = 100
ESPERA_TANDA_SEGUNDOS = 1.0
INTERVALO_LIMPIEZA_SEGUNDOS = 3600


@dataclass
class Llamada:
    correlation_id: Optional[str]
    origen: str
    destino: str
    metodo: str
    ruta: str
    codigo_http: Optional[int]
    latencia_ms: int
    error: Optional[str]
    ocurrida_en: datetime


class RegistroAuditoria:
    def __init__(self, settings: Settings, tamanio_cola: int = 1000) -> None:
        self.settings = settings
        self._cola: asyncio.Queue[Llamada] = asyncio.Queue(maxsize=tamanio_cola)
        self._tareas: list[asyncio.Task] = []
        self.descartadas = 0

    @property
    def habilitado(self) -> bool:
        return obtener_sessionmaker() is not None

    def registrar(self, llamada: Llamada) -> None:
        if not self.habilitado:
            return
        try:
            self._cola.put_nowait(llamada)
        except asyncio.QueueFull:
            self.descartadas += 1
            if self.descartadas % 100 == 1:
                logger.warning("Cola de auditoría llena: %s registros descartados.", self.descartadas)

    async def iniciar(self) -> None:
        if not self.habilitado:
            logger.info("Auditoría deshabilitada: no hay base de datos configurada.")
            return
        self._tareas = [
            asyncio.create_task(self._escribir()),
            asyncio.create_task(self._limpiar_periodicamente()),
        ]

    async def detener(self) -> None:
        for tarea in self._tareas:
            tarea.cancel()
        for tarea in self._tareas:
            try:
                await tarea
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tareas = []
        # Lo que quedó en la cola se intenta guardar antes de apagar.
        pendientes = self._vaciar_cola()
        if pendientes:
            await self._insertar(pendientes)

    def _vaciar_cola(self, limite: Optional[int] = None) -> list[Llamada]:
        tanda: list[Llamada] = []
        while not self._cola.empty() and (limite is None or len(tanda) < limite):
            tanda.append(self._cola.get_nowait())
        return tanda

    async def _escribir(self) -> None:
        while True:
            primera = await self._cola.get()
            await asyncio.sleep(ESPERA_TANDA_SEGUNDOS)  # junta lo que llegue mientras tanto
            tanda = [primera, *self._vaciar_cola(TAMANIO_TANDA - 1)]
            await self._insertar(tanda)

    async def _insertar(self, tanda: list[Llamada]) -> None:
        sessionmaker = obtener_sessionmaker()
        if sessionmaker is None or not tanda:
            return
        try:
            async with sessionmaker() as session:
                await session.execute(insert(LlamadaServicio), [asdict(l) for l in tanda])
                await session.commit()
        except Exception as exc:  # noqa: BLE001 - la auditoría no puede tumbar al orquestador
            logger.warning("No se pudieron guardar %s registros de auditoría: %s", len(tanda), exc)

    async def _limpiar_periodicamente(self) -> None:
        while True:
            await self.limpiar()
            await asyncio.sleep(INTERVALO_LIMPIEZA_SEGUNDOS)

    async def limpiar(self) -> None:
        sessionmaker = obtener_sessionmaker()
        if sessionmaker is None:
            return
        limite = datetime.now(timezone.utc) - timedelta(days=self.settings.auditoria_retencion_dias)
        try:
            async with sessionmaker() as session:
                resultado = await session.execute(
                    delete(LlamadaServicio).where(LlamadaServicio.ocurrida_en < limite)
                )
                await session.commit()
            if resultado.rowcount:
                logger.info("Auditoría: %s registros viejos eliminados.", resultado.rowcount)
        except Exception as exc:  # noqa: BLE001
            logger.warning("No se pudo limpiar la auditoría: %s", exc)
