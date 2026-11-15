"""Los comandos que el orquestador le publica a los servicios (docs/08-contrato-tres-topicos.md §3).

Cada comando se anota primero en el outbox y lo publica el despachador en `crm.comandos`,
con el `destino` que corresponde. El payload de los que reenvían datos de un evento se guarda
en el outbox **solo mientras está pendiente**; `GenerarResumen` no lleva payload: lo arma el
despachador al publicar, consultando a leads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from app.config import Settings

CALIFICAR_LEAD = "CalificarLead"
REGISTRAR_CALIFICACION = "RegistrarCalificacion"
ACTUALIZAR_CALIFICACION = "ActualizarCalificacion"
GENERAR_RESUMEN = "GenerarResumen"
GUARDAR_RESUMEN = "GuardarResumen"
# Flujo de multas: ConsultarMultas va por Kafka al scraper; GuardarMultas se entrega a vehiculos por REST
# (PUT /vehiculos/{id}/multas), pero pasa por el mismo outbox para no perderse si vehiculos está caído.
CONSULTAR_MULTAS = "ConsultarMultas"
GUARDAR_MULTAS = "GuardarMultas"
# El `topic` del outbox para los comandos que no van por Kafka.
CANAL_REST_VEHICULOS = "REST:vehiculos"

# Para registrar en el flujo que un comando sin paso propio falló: comando -> (paso, servicio).
PASO_DEL_COMANDO: dict[str, tuple[str, str]] = {
    REGISTRAR_CALIFICACION: ("REGISTRAR_CALIFICACION", "clientes"),
    ACTUALIZAR_CALIFICACION: ("ACTUALIZAR_CALIFICACION", "leads"),
    GUARDAR_RESUMEN: ("GUARDAR_RESUMEN", "leads"),
}


@dataclass(frozen=True)
class ComandoSaliente:
    """Lo que un evento le pide al orquestador que publique después."""

    topic: str
    event_type: str
    destino: str
    # Datos a reenviar; None si el despachador los arma al publicar.
    payload: Optional[dict[str, Any]] = None
    # Si ya hay uno igual pendiente para el lead, no se anota otro.
    unico: bool = False
    # No se publica antes de que pase este tiempo (espera entre reintentos de un servicio).
    demora_segundos: float = 0.0


def calificar_lead(settings: Settings, payload: dict[str, Any]) -> ComandoSaliente:
    return ComandoSaliente(settings.kafka_topico_comandos, CALIFICAR_LEAD, "calificacion", payload, unico=True)


def registrar_calificacion(settings: Settings, payload: dict[str, Any]) -> ComandoSaliente:
    return ComandoSaliente(settings.kafka_topico_comandos, REGISTRAR_CALIFICACION, "clientes", payload)


def actualizar_calificacion(settings: Settings, payload: dict[str, Any]) -> ComandoSaliente:
    return ComandoSaliente(settings.kafka_topico_comandos, ACTUALIZAR_CALIFICACION, "leads", payload)


def generar_resumen(settings: Settings) -> ComandoSaliente:
    return ComandoSaliente(settings.kafka_topico_comandos, GENERAR_RESUMEN, "ia-resumen", unico=True)


def guardar_resumen(settings: Settings, payload: dict[str, Any]) -> ComandoSaliente:
    return ComandoSaliente(settings.kafka_topico_comandos, GUARDAR_RESUMEN, "leads", payload)


def consultar_multas(settings: Settings, demora_segundos: float = 0.0) -> ComandoSaliente:
    """Sin payload: el despachador lo arma al publicar (vehiculoId y patente, consultando a vehiculos)."""
    return ComandoSaliente(
        settings.kafka_topico_comandos, CONSULTAR_MULTAS, "multas", unico=True, demora_segundos=demora_segundos
    )


def guardar_multas(payload: dict[str, Any]) -> ComandoSaliente:
    """El resultado del scraper, tal cual, para vehiculos. Se guarda en el outbox solo mientras está pendiente."""
    return ComandoSaliente(CANAL_REST_VEHICULOS, GUARDAR_MULTAS, "vehiculos", payload)
