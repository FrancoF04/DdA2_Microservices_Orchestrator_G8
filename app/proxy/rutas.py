"""Tabla de ruteo del API Gateway.

El primer segmento después de `/api` es el recurso; el resto de la ruta se
reenvía sin modificar (docs/02-propuesta-orquestador.md §3.1):

    /api/leads/123/state  ->  {LEADS_URL}/leads/123/state
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.config import Settings


@dataclass(frozen=True)
class Destino:
    servicio: str
    url_base: Optional[str]


# recurso (primer segmento de la ruta) -> (nombre del servicio, atributo de Settings con su URL)
RECURSOS: dict[str, tuple[str, str]] = {
    "leads": ("leads", "leads_url"),
    "clientes": ("clientes", "clientes_url"),
    "vehiculos": ("vehiculos", "vehiculos_url"),
    "calificaciones": ("calificacion", "calificacion_url"),
    "monitor": ("monitor", "monitor_url"),
    "resumen": ("ia-resumen", "ia_resumen_url"),
    "resumen-ia": ("ia-resumen", "ia_resumen_url"),
    "auth": ("auth", "auth_url"),
    "multas": ("multas", "multas_url"),
}


def resolver_destino(recurso: str, settings: Settings) -> Optional[Destino]:
    entrada = RECURSOS.get(recurso)
    if entrada is None:
        return None
    servicio, atributo = entrada
    return Destino(servicio=servicio, url_base=getattr(settings, atributo))


def servicios_configurados(settings: Settings) -> dict[str, Optional[str]]:
    """servicio -> URL base (None si no está configurado). Sin duplicados."""
    resultado: dict[str, Optional[str]] = {}
    for servicio, atributo in RECURSOS.values():
        resultado.setdefault(servicio, getattr(settings, atributo))
    return resultado
