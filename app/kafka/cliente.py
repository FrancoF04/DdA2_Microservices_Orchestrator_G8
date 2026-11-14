"""Ciclo de vida de Kafka: producer, consumer y estado para /health.

Si el broker no está disponible al arrancar, la API levanta igual (como
ia-resumen) y una tarea de fondo reintenta la conexión cada
`kafka_reintento_conexion_segundos`.

El consumer se suscribe a los tópicos que tengan un manejador registrado en
`MANEJADORES`. En la fase 0 no hay ninguno; los agrega la fase 1b (seguimiento de flujos).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from app.config import Settings
from app.kafka.config import TOPICOS_PROPIOS, argumentos_seguridad
from app.kafka.eventos import armar_sobre

logger = logging.getLogger(__name__)

Manejador = Callable[[dict[str, Any], Any], Awaitable[None]]

# tópico -> función async (mensaje_decodificado, record_kafka)
MANEJADORES: dict[str, Manejador] = {}

DESHABILITADO = "DESHABILITADO"
CONECTANDO = "CONECTANDO"
UP = "UP"
DOWN = "DOWN"


class ClienteKafka:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.producer: Optional[AIOKafkaProducer] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.estado: str = DESHABILITADO if not settings.kafka_enabled else CONECTANDO
        self.ultimo_error: Optional[str] = None
        self._tarea_conexion: Optional[asyncio.Task] = None
        self._tarea_consumo: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ ciclo de vida

    async def iniciar(self) -> None:
        if not self.settings.kafka_enabled:
            logger.info("Kafka deshabilitado (KAFKA_ENABLED=false).")
            return
        self._tarea_conexion = asyncio.create_task(self._conectar_con_reintentos())

    async def detener(self) -> None:
        for tarea in (self._tarea_conexion, self._tarea_consumo):
            if tarea is not None:
                tarea.cancel()
                try:
                    await tarea
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass
        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None
        if self.producer is not None:
            await self.producer.stop()
            self.producer = None

    async def _conectar_con_reintentos(self) -> None:
        while True:
            try:
                await self._conectar()
                self.estado = UP
                self.ultimo_error = None
                logger.info("Kafka conectado: %s", self.settings.kafka_bootstrap_servers)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.estado = DOWN
                self.ultimo_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "No se pudo conectar a Kafka (%s). Reintento en %ss.",
                    self.ultimo_error,
                    self.settings.kafka_reintento_conexion_segundos,
                )
                await self._limpiar_parcial()
                await asyncio.sleep(self.settings.kafka_reintento_conexion_segundos)

    async def _conectar(self) -> None:
        seguridad = argumentos_seguridad(self.settings)
        await self._asegurar_topicos(seguridad)

        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=self.settings.kafka_client_id,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            **seguridad,
        )
        await self.producer.start()

        if MANEJADORES:
            self.consumer = AIOKafkaConsumer(
                *MANEJADORES.keys(),
                bootstrap_servers=self.settings.kafka_bootstrap_servers,
                group_id=self.settings.kafka_consumer_group,
                client_id=self.settings.kafka_client_id,
                auto_offset_reset="earliest",
                # Commit manual, después de procesar (no se pierden pasos si se cae).
                enable_auto_commit=False,
                **seguridad,
            )
            await self.consumer.start()
            self._tarea_consumo = asyncio.create_task(self._consumir())
        else:
            logger.info("Sin manejadores de Kafka registrados: no se inicia el consumer.")

    async def _limpiar_parcial(self) -> None:
        for cliente in (self.consumer, self.producer):
            if cliente is not None:
                try:
                    await cliente.stop()
                except Exception:  # noqa: BLE001
                    pass
        self.consumer = None
        self.producer = None

    async def _asegurar_topicos(self, seguridad: dict[str, Any]) -> None:
        admin = AIOKafkaAdminClient(
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            client_id=f"{self.settings.kafka_client_id}-admin",
            **seguridad,
        )
        await admin.start()
        try:
            existentes = set(await admin.list_topics())
            nuevos = [
                NewTopic(name=t, num_partitions=3, replication_factor=1)
                for t in TOPICOS_PROPIOS
                if t not in existentes
            ]
            if nuevos:
                await admin.create_topics(new_topics=nuevos)
                logger.info("Tópicos creados: %s", [n.name for n in nuevos])
        except Exception as exc:  # noqa: BLE001 - sin permisos no debe frenar el arranque
            logger.warning("No se pudieron asegurar los tópicos propios: %s", exc)
        finally:
            await admin.close()

    # ------------------------------------------------------------------ consumo

    async def _consumir(self) -> None:
        assert self.consumer is not None
        async for record in self.consumer:
            manejador = MANEJADORES.get(record.topic)
            try:
                mensaje = json.loads(record.value.decode("utf-8"))
                if manejador is not None:
                    await manejador(mensaje, record)
            except json.JSONDecodeError as exc:
                logger.error("JSON inválido en %s@%s: %s", record.topic, record.offset, exc)
            except Exception as exc:  # noqa: BLE001
                # Fase 1b: marcar el flujo con el error (sin tópico DLT propio por el límite de Aiven).
                logger.exception("Error procesando %s@%s: %s", record.topic, record.offset, exc)
            await self.consumer.commit()

    # ------------------------------------------------------------------ publicación

    async def publicar(
        self,
        topico: str,
        clave: str,
        event_type: str,
        payload: dict[str, Any],
        correlation_id: Optional[str] = None,
    ) -> dict[str, Any]:
        if self.producer is None:
            raise RuntimeError(f"Kafka no disponible (estado {self.estado}).")
        sobre = armar_sobre(event_type, payload, correlation_id)
        await self.producer.send_and_wait(topico, key=clave.encode("utf-8"), value=sobre)
        return sobre
