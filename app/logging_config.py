"""Logging con el correlation id de la request en cada línea."""

from __future__ import annotations

import logging

from app.middleware.correlation_id import correlation_id_actual


class _CorrelationIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = correlation_id_actual() or "-"
        return True


def configurar_logging(nivel: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.addFilter(_CorrelationIdFilter())
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s [%(correlation_id)s] %(name)s: %(message)s"
        )
    )
    raiz = logging.getLogger()
    raiz.handlers = [handler]
    raiz.setLevel(nivel.upper())
