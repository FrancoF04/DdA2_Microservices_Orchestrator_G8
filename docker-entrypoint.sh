#!/bin/sh
set -e

# Aplica las migraciones pendientes antes de levantar la API.
# Se puede desactivar con MIGRAR_AL_INICIAR=false (ej. si las corre otro proceso).
if [ "${MIGRAR_AL_INICIAR:-true}" = "true" ] && [ -n "${DATABASE_URL:-}" ]; then
    echo "Aplicando migraciones de base de datos..."
    alembic upgrade head
fi

exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8080}" --proxy-headers
