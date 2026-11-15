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
