# DdA2_Microservices_Orchestrator_G8

Orquestador y API Gateway del ecosistema de microservicios del TP de Desarrollo de Aplicaciones II (UADE, grupo 8). Hecho con FastAPI, Kafka (aiokafka) y PostgreSQL (Supabase).

La documentación de diseño está en [docs/](docs/README.md).

## Requisitos

- Python 3.13
- Docker Desktop (para el compose local)

## Entorno local

Este repo trae la **infraestructura local de todo el ecosistema**: Postgres (un schema por servicio), Kafka, kafka-ui y, opcional, un simulador de `clientes` (hay servicio real). Los servicios se corren nativos desde su propio repo. La guía completa, con el `.env` de cada repo, está en [docs/04-desarrollo-local.md](docs/04-desarrollo-local.md).

```bash
docker compose up -d          # infraestructura
```

Para correr el orquestador:

```bash
py -3.13 -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt -c constraints.txt
cp .env.example .env            # y completar (ver docs/04)
alembic upgrade head
python -m app --reload          # en Windows usar esto y no `uvicorn` directo
```

Otra opción es levantar el orquestador en contenedor, con `docker compose --profile orquestador up -d`.

Sin `DATABASE_URL` y con `KAFKA_ENABLED=false`, la app levanta igual: `/health` informa esas dependencias como `DESHABILITADO`.

Para correr contra Supabase y Aiven, completá `.env` y levantá solo la imagen:

```bash
docker build -t orquestador:local .
docker run --env-file .env -p 8080:8080 orquestador:local
```

## Tests

```bash
pytest
```

## Endpoints

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado del orquestador, la base y Kafka (`UP` / `DEGRADADO`) |
| * | `/api/{recurso}/**` | Proxy hacia el servicio dueño del recurso: `leads`, `clientes`, `vehiculos`, `calificaciones`, `monitor`, `resumen`, `resumen-ia`, `auth`, `multas` |
| GET | `/api/orquestador/servicios?ventana_minutos=60` | Por servicio: llamadas, % de error, latencia p50/p95 y último error |
| GET | `/api/orquestador/flujos?leadId=&estado=&tipo=` | Flujos y sus pasos. Ej.: `?estado=EXPIRADO` para ver los trabados |
| GET | `/api/orquestador/flujos/{id}` | Un flujo con todos sus pasos y latencias |
| GET | `/docs` | Swagger UI |

Ejemplo: `GET /api/leads?estado=CALIFICADO` se reenvía a `{LEADS_URL}/leads?estado=CALIFICADO`.

Para desplegar en Render y conectar los demás servicios, ver [docs/03-despliegue-render.md](docs/03-despliegue-render.md).

## Dependencias

Todas las versiones están fijadas, y cada una tiene al menos 3 días de publicada (ver [CLAUDE.md](CLAUDE.md)):

- Las directas, en `requirements.txt`.
- Las transitivas, en `constraints.txt`.
- Las imágenes de Docker, por tag y digest.
