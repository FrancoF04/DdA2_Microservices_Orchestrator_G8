"""Configuración de Kafka (local sin auth o Aiven con SASL_SSL).

Mismo enfoque que leads: aiokafka no acepta `ssl_cafile`, hay que armar un
`ssl.SSLContext` y pasarlo como `ssl_context`.
"""

from __future__ import annotations

import ssl
from typing import Any

from app.config import Settings

TOPICO_DLT_ORQUESTADOR = "orquestador-dlt"

# Tópicos que el orquestador crea si no existen (Aiven no los autocrea).
# Solo los propios: los tópicos de los demás servicios los crean sus dueños.
TOPICOS_PROPIOS = [TOPICO_DLT_ORQUESTADOR]


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
