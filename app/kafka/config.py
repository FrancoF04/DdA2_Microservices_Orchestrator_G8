"""Configuración de Kafka (local sin auth o Aiven con SASL_SSL).

Mismo enfoque que leads: aiokafka no acepta `ssl_cafile`, hay que armar un
`ssl.SSLContext` y pasarlo como `ssl_context`.
"""

from __future__ import annotations

import ssl
from typing import Any

from app.config import Settings

def topicos_propios(settings: Settings) -> list[str]:
    """Los tres tópicos del ecosistema, que el orquestador crea si no existen (Aiven no los autocrea)."""
    return [settings.kafka_topico_comandos, settings.kafka_topico_eventos, settings.kafka_topico_dlt]


def argumentos_seguridad(settings: Settings) -> dict[str, Any]:
    if not settings.kafka_security_protocol:
        return {}
    args: dict[str, Any] = {"security_protocol": settings.kafka_security_protocol}
    if settings.kafka_sasl_mechanism:
        args["sasl_mechanism"] = settings.kafka_sasl_mechanism
    if settings.kafka_sasl_username:
        args["sasl_plain_username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password:
        args["sasl_plain_password"] = settings.kafka_sasl_password
    if settings.kafka_ssl_cafile:
        args["ssl_context"] = ssl.create_default_context(cafile=settings.kafka_ssl_cafile)
    elif "SSL" in settings.kafka_security_protocol.upper():
        args["ssl_context"] = ssl.create_default_context()
    return args
