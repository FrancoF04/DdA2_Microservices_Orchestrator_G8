# Imagen fijada por tag y digest (versión con >= 3 días de publicada, ver CLAUDE.md).
FROM python:3.13.18-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

RUN groupadd --system --gid 10001 orquestador \
    && useradd --system --uid 10001 --gid orquestador --no-create-home --shell /usr/sbin/nologin orquestador

COPY requirements.txt constraints.txt ./
RUN pip install -r requirements.txt -c constraints.txt

COPY alembic.ini ./
COPY alembic ./alembic
COPY app ./app
# CA pública de Aiven para Kafka SASL_SSL (KAFKA_SSL_CAFILE=ca.pem).
COPY ca.pem docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

USER orquestador
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=4 \
    CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://localhost:8080/health', timeout=4).status == 200 else 1)"

ENTRYPOINT ["./docker-entrypoint.sh"]
