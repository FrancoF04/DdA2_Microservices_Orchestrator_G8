# Desarrollo local del ecosistema

Cómo levantar todo el sistema en una sola máquina para desarrollar y probar: la **infraestructura** (Postgres y Kafka) en Docker, y los **servicios** nativos, cada uno desde su repo.

Todos los repos tienen que estar uno al lado del otro en la misma carpeta (como `DdA2_Repos/`).

## 1. Levantar la infraestructura

Desde este repo:

```bash
docker compose up -d
```

| Qué | Dónde | Notas |
|---|---|---|
| PostgreSQL | `localhost:5432` | Una sola base (`postgres`) con **un schema y un usuario por servicio**, como en Supabase. Usuario = contraseña = nombre del servicio. Si ya tenés un Postgres en 5432: `POSTGRES_PUERTO=5433 docker compose up -d`. |
| Kafka | `localhost:9092` | Sin autenticación y **sin límite de tópicos**. |
| kafka-ui | <http://localhost:8090> | Para ver tópicos y mensajes. |
| clientes (simulado) | `http://localhost:8082` | Reemplaza al servicio real hasta que exista. Ver [dev/README.md](../dev/README.md). |

Schemas creados: `leads`, `calificacion`, `vehiculos`, `clientes`, `monitor`, `orquestador`, `auth`, `multas`.

- **Para apagar:** `docker compose down`. Los datos quedan en volúmenes.
- **Para borrar todo y empezar de cero:** `docker compose down -v`.

> **No levantar a la vez el `docker-compose.yml` propio de LeadsMicroservice**: trae su propio Postgres y Kafka en los mismos puertos.

## 2. Levantar cada servicio

Valores para el `.env` local de cada repo (o variables de entorno). Las URLs entre servicios apuntan al **orquestador** (`http://127.0.0.1:8080/api`), para que todo el tráfico pase por él.

> **En Windows usar `127.0.0.1` y no `localhost`** en las URLs entre servicios: `localhost` prueba primero IPv6 y, si el servicio no escucha ahí, tarda unos 2 s extra en cada conexión.

### Puertos

| Servicio | Puerto |
|---|---|
| orquestador | 8080 |
| leads | 8000 |
| ia-resumen | **8001** (no 8000, que choca con leads) |
| clientes | 8082 |
| vehiculos | 8083 |
| calificacion | 8084 |
| monitor | 8085 |
| front | 5173 |

### Orquestador (este repo)

```dotenv
DATABASE_URL=postgresql://orquestador:orquestador@127.0.0.1:5432/postgres
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
LEADS_URL=http://127.0.0.1:8000
CLIENTES_URL=http://127.0.0.1:8082
VEHICULOS_URL=http://127.0.0.1:8083
CALIFICACION_URL=http://127.0.0.1:8084
MONITOR_URL=http://127.0.0.1:8085
IA_RESUMEN_URL=http://127.0.0.1:8001
```

```bash
alembic upgrade head
python -m app --reload
```

`python -m app` y no `uvicorn app.main:app`: en Windows el loop de eventos por defecto no es compatible con el driver de Postgres (psycopg async) y `python -m app` elige el correcto. En Linux da igual.

Alternativa sin instalar Python: `docker compose --profile orquestador up -d` (el orquestador en contenedor, que alcanza a los servicios nativos por `host.docker.internal`).

### LeadsMicroservice

```dotenv
# postgresql+psycopg:// fija el driver (el repo instala psycopg 3). Con postgresql:// a secas
# funciona en SQLAlchemy 2.1, pero en 2.0 busca psycopg2 y falla; leads no fija la versión.
DATABASE_URL=postgresql+psycopg://leads:leads@127.0.0.1:5432/postgres
FRONTEND_LIST=["http://localhost:5173"]
CLIENTES_URL=http://127.0.0.1:8080/api
CLIENTES_CLAVE_LEADS=clave-local-leads
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
```

```bash
uvicorn app.main:app --port 8000 --reload
```

Las tablas se crean solas en el schema `leads` (el usuario `leads` lo tiene como `search_path`).

### IAllmFormulario (ia-resumen)

```dotenv
CLIENTES_URL=http://127.0.0.1:8080/api
VEHICULOS_URL=http://127.0.0.1:8080/api
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
# Vacías para el Kafka local (el .env.example trae SASL_SSL de Aiven):
KAFKA_SECURITY_PROTOCOL=
KAFKA_SASL_MECHANISM=
KAFKA_SSL_CAFILE=
# Sin clave no hay resumen con IA (el consumer falla al generar)
OLLAMA_API_KEY=<pedir al responsable>
```

```bash
uvicorn app.main:app --port 8001 --reload
```

### CalificacionMicroservice (requiere Maven)

```dotenv
SPRING_DATASOURCE_URL=jdbc:postgresql://127.0.0.1:5432/postgres
SPRING_DATASOURCE_USERNAME=calificacion
SPRING_DATASOURCE_PASSWORD=calificacion
SPRING_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
CLIENTES_SERVICE_URL=http://127.0.0.1:8080/api
VEHICULOS_SERVICE_URL=http://127.0.0.1:8080/api
CLIENTES_CLAVE_CALIFICACION=clave-local-calificacion
```

```bash
mvn spring-boot:run
```

### VehiculosMicroservice (requiere Maven)

Contra el Postgres local **sí** se puede usar el perfil `seed` (datos de prueba). La prohibición del README de ese repo es solo para la base compartida de Supabase.

```dotenv
SPRING_PROFILES_ACTIVE=local,seed
VEHICULOS_DB_URL=jdbc:postgresql://127.0.0.1:5432/postgres
VEHICULOS_DB_USUARIO=vehiculos
VEHICULOS_DB_CLAVE=vehiculos
VEHICULOS_FOTOS_MODO=deshabilitado
VEHICULOS_FOTOS_BUCKET=vehiculos-fotos
VEHICULOS_CLAVE_ESCRITURA=clave-local-vehiculos-de-32-caracteres-o-mas
```

```bash
mvn -pl vehiculos-service spring-boot:run
```

El seed incluye el Toyota Corolla `a0000000-0000-4000-8000-000000000001`, útil como `vehiculoId` de prueba.

### MonitorMicroservice (requiere Maven)

El monitor sondea a cada servicio **directo**, no por el orquestador.

```dotenv
SPRING_DATASOURCE_URL=jdbc:postgresql://127.0.0.1:5432/postgres
SPRING_DATASOURCE_USERNAME=monitor
SPRING_DATASOURCE_PASSWORD=monitor
SPRING_DATASOURCE_DRIVER=org.postgresql.Driver
VEHICULOS_SERVICE_URL=http://127.0.0.1:8083
CLIENTES_SERVICE_URL=http://127.0.0.1:8082
LEADS_SERVICE_URL=http://127.0.0.1:8000
CALIFICACION_SERVICE_URL=http://127.0.0.1:8084
```

Sin estas variables usa H2 en memoria y también funciona.

### DDA2 (front)

```dotenv
VITE_LEADS_API_URL=http://localhost:8080/api
VITE_VEHICULOS_API_URL=http://localhost:8080/api
VITE_CALIFICACION_API_URL=http://localhost:8080/api
VITE_CLIENTES_API_URL=http://localhost:8080/api
VITE_MONITOR_API_URL=http://localhost:8080/api
```

El orquestador ya permite el origen `http://localhost:5173` en CORS. El sondeo de respaldo de la página Monitor no funciona con estas URLs (ver [03-despliegue-render.md §4](03-despliegue-render.md#nota-sondeo-de-respaldo-del-front)).

## 3. Probar el flujo completo

1. Enviar un preformulario por el orquestador:

   ```bash
   curl -X POST http://127.0.0.1:8080/api/leads -H "Content-Type: application/json" -H "X-Correlation-Id: prueba-1" -d @preformulario.json
   ```

   Usar un `vehiculoId` existente (ej. el Corolla del seed).
2. Mirar en kafka-ui (<http://localhost:8090>) los mensajes de `preformulario-completado`, `lead-calificado`, `resumen-solicitado` y `resumen-generado`.
3. Ver las llamadas que pasaron por el orquestador:

   ```bash
   curl "http://127.0.0.1:8080/api/orquestador/servicios?ventana_minutos=60"
   ```

   O directo en la base: `select * from orquestador.llamada_servicio order by id;`

### Simular un servicio que no está levantado

Para probar un tramo sin tener todos los servicios, se puede publicar a mano el evento que publicaría el servicio faltante. Por ejemplo, el `lead-calificado` de calificacion (que se publica **sin sobre**, JSON plano):

```bash
echo '<leadId>|{"leadId":"<leadId>","clienteId":"<clienteId>","puntos":55,"nivel":"POTABLE","origen":"AUTOMATICA","motivos":[]}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic lead-calificado \
      --property parse.key=true --property "key.separator=|"
```

En Git Bash anteponer `MSYS_NO_PATHCONV=1` a los comandos `docker compose exec`, porque si no Git Bash reescribe las rutas `/opt/...`.

## 4. Problemas conocidos

| Síntoma | Causa | Solución |
|---|---|---|
| El orquestador informa la base `DOWN` con `ProactorEventLoop` en el log | Se levantó con `uvicorn` en Windows | Usar `python -m app` |
| leads no arranca: `No module named 'psycopg2'` | `DATABASE_URL` con `postgresql://` y SQLAlchemy 2.0 instalado | Usar `postgresql+psycopg://` |
| Cada llamada entre servicios tarda ~2 s de más | `localhost` en Windows | Usar `127.0.0.1` |
| `port is already allocated` al levantar el compose | Otro Postgres o Kafka en 5432/9092 (por ejemplo el compose de leads) | Apagar el otro, o `POSTGRES_PUERTO=5433` |
| Los schemas no existen | El volumen de Postgres ya tenía datos de antes del script de inicialización | `docker compose down -v` y volver a levantar |
