# DdA2_Microservices_Orchestrator_G8

Orquestador y API Gateway del ecosistema de microservicios del TP de Desarrollo de Aplicaciones II (UADE, grupo 8). Hecho con FastAPI, Kafka (aiokafka) y PostgreSQL (Supabase).

La documentación de diseño está en [docs/](docs/README.md).

## Requisitos

- Python 3.13
- Docker Desktop (para el compose local)

## Correr con Docker (recomendado)

```bash
cp .env.example .env
docker compose up --build
```

Levanta tres contenedores con infraestructura local, sin depender de Supabase ni de Aiven:

- `orquestador` en <http://localhost:8080>
- PostgreSQL en el puerto `5433`
- Kafka (KRaft) en `localhost:9092`

Las migraciones se aplican solas al arrancar. Para inspeccionar los tópicos, agregá kafka-ui en <http://localhost:8090>:

```bash
docker compose --profile herramientas up
```

Para correr contra Supabase y Aiven, completá `.env` y levantá solo la imagen:

```bash
docker build -t orquestador:local .
docker run --env-file .env -p 8080:8080 orquestador:local
```

## Correr sin Docker

```bash
py -3.13 -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
pip install -r requirements-dev.txt -c constraints.txt
alembic upgrade head            # requiere DATABASE_URL
uvicorn app.main:app --reload --port 8080
```

Sin `DATABASE_URL` y con `KAFKA_ENABLED=false`, la app levanta igual: `/health` informa esas dependencias como `DESHABILITADO`.

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
| GET | `/docs` | Swagger UI |

Ejemplo: `GET /api/leads?estado=CALIFICADO` se reenvía a `{LEADS_URL}/leads?estado=CALIFICADO`.

Para desplegar en Render y conectar los demás servicios, ver [docs/03-despliegue-render.md](docs/03-despliegue-render.md).

## Dependencias

Todas las versiones están fijadas, y cada una tiene al menos 3 días de publicada (ver [CLAUDE.md](CLAUDE.md)):

- Las directas, en `requirements.txt`.
- Las transitivas, en `constraints.txt`.
- Las imágenes de Docker, por tag y digest.
