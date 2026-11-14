#!/bin/sh
set -e

# Aplica las migraciones pendientes antes de levantar la API.
# Se puede desactivar con MIGRAR_AL_INICIAR=false (ej. si las corre otro proceso).
if [ "${MIGRAR_AL_INICIAR:-true}" = "true" ] && [ -n "${DATABASE_URL:-}" ]; then
    echo "Aplicando migraciones de base de datos..."
    alembic upgrade head
fi

# FORWARDED_ALLOW_IPS: de qué proxies se aceptan los headers X-Forwarded-*.
# En Render va "*" (el proxy de Render no tiene IP fija); en local, solo localhost.
exec uvicorn app.main:app \
    --host 0.0.0.0 \
    --port "${PORT:-8080}" \
    --proxy-headers \
    --forwarded-allow-ips "${FORWARDED_ALLOW_IPS:-127.0.0.1}"
