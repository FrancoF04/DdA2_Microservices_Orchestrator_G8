# Desarrollo local del ecosistema

Cómo levantar todo el sistema en una sola máquina para desarrollar y probar: la **infraestructura** (Postgres y Kafka) en Docker, y los **servicios** nativos, cada uno desde su repo.

Todos los repos tienen que estar uno al lado del otro en la misma carpeta (como `DdA2_Repos/`).

> **¿Desplegar en la nube (Aiven + Supabase + Render)?** Eso es otro documento: [09-despliegue-desde-cero.md](09-despliegue-desde-cero.md).

## Paso a paso (resumen)

Cada paso tiene su detalle más abajo. El orden importa: los servicios crean sus tablas y sus tópicos al arrancar, y el orquestador tiene que estar arriba para que los demás se llamen entre sí.

1. **Infraestructura:** `docker compose up -d` en este repo (Postgres con un schema por servicio y Kafka). Ver §1.
2. **Un `.env` por servicio Python** y **variables de entorno para los Java**, con los valores de §2 (ya coinciden entre sí: claves de servicio, `JWT_SECRET`, URLs).
3. **Levantar, en este orden**, cada uno en su terminal:

   | # | Servicio | Comando (desde su repo) |
   |---|---|---|
   | 1 | orquestador | `alembic upgrade head` y `python -m app --reload` |
   | 2 | auth | `uvicorn app.main:app --port 8086 --reload` |
   | 3 | vehiculos | `./mvnw -pl vehiculos-service spring-boot:run` (con el perfil `seed`) |
   | 4 | clientes | `./mvnw -pl clientes-service spring-boot:run` (la 1ra vez antes: `./mvnw -pl contracts -am install -DskipTests`) |
   | 5 | calificacion | `./mvnw spring-boot:run` |
   | 6 | leads | `uvicorn app.main:app --port 8000 --reload` |
   | 7 | ia-resumen | `uvicorn app.main:app --port 8001 --reload` |
   | 8 | multas | `pnpm start` (la 1ra vez: `pnpm install` y `pnpm exec playwright install chromium`) |
   | 9 | monitor | `./mvnw spring-boot:run` |
   | 10 | front | `pnpm dev` |

   En PowerShell, `.\mvnw.cmd` en vez de `./mvnw`.
4. **Probar todo de una** (lead de punta a punta y multas de la unidad AAA111):

   ```bash
   python scripts/verificar_despliegue.py --gateway http://127.0.0.1:8080 --monitor http://127.0.0.1:8085 \
       --email admin@example.com --password admin-local-123        # agregar --sin-llm si no hay OLLAMA_API_KEY
   ```

   O paso por paso con `curl`, como en §3. Para entrar al front: <http://localhost:5173> con `admin@example.com` / `admin-local-123`.

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
| clientes (simulado, **opcional**) | `http://localhost:8082` | Solo si no levantás el servicio real: `docker compose --profile clientes-simulado up -d`. Ver [dev/README.md](../dev/README.md). Con el real en marcha, no se usa. |

Schemas creados: `leads`, `calificacion`, `vehiculos`, `clientes`, `monitor`, `orquestador`, `auth`, `multas`.

- **Para apagar:** `docker compose down`. Los datos quedan en volúmenes.
- **Para borrar todo y empezar de cero:** `docker compose down -v`.

> **No levantar a la vez el `docker-compose.yml` propio de LeadsMicroservice**: trae su propio Postgres y Kafka en los mismos puertos.

## 2. Levantar cada servicio

Estas son las variables de cada servicio. Las URLs entre servicios apuntan al **orquestador** (`http://127.0.0.1:8080/api`), para que todo el tráfico pase por él.

- **Servicios Python (orquestador, leads, ia-resumen):** van en un `.env` en la raíz del repo, que se lee solo al arrancar.
- **Servicios Java (clientes, calificacion, vehiculos, monitor):** Spring Boot **no lee** archivos `.env`. Hay que cargarlas como variables de entorno en la terminal antes del `./mvnw`, o en la configuración de ejecución del IDE (en IntelliJ: *Run/Debug Configurations → Environment variables*). Vehiculos trae scripts para eso (`scripts/cargar-env.ps1` y `scripts/cargar-env.sh`), que leen `infra/.env`.
- **Los cuatro repos Java traen el Maven Wrapper:** alcanza con JDK 21 o superior, no hace falta instalar Maven.

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
| auth | 8086 |
| multas | 8087 |
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
AUTH_URL=http://127.0.0.1:8086
MULTAS_URL=http://127.0.0.1:8087
# Flujo de multas (docs/08 §7): la misma clave que VEHICULOS_CLAVE_ESCRITURA de vehiculos.
VEHICULOS_API_KEY=clave-local-vehiculos-de-32-caracteres-o-mas
# Opcionales: consulta de multas automática al alta de una unidad y barrido diario.
# MULTAS_AL_ALTA=true  MULTAS_BARRIDO_HABILITADO=true
# Autenticación (docs/06-autenticacion.md): sin JWT_SECRET el orquestador NO arranca.
# El mismo JWT_SECRET que AuthMicroservice. Claves de servicio: 32+ caracteres y distintas.
JWT_SECRET=local-jwt-secret-solo-desarrollo-0123456789abcdef
CLAVES_SERVICIO=leads=clave-gateway-leads-local-0000000000001,calificacion=clave-gateway-calificacion-local-00000001,ia-resumen=clave-gateway-ia-resumen-local-0000000001
# Para trabajar sin login (solo desarrollo): AUTH_HABILITADA=false
# Los tres tópicos del ecosistema (docs/08-contrato-tres-topicos.md): son los valores por defecto, no hace falta
# definirlos. Los crea el orquestador al arrancar (en el Kafka local no hay límite de tópicos).
# KAFKA_TOPICO_COMANDOS=crm.comandos  KAFKA_TOPICO_EVENTOS=crm.eventos  KAFKA_TOPICO_DLT=crm.dlt
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
CLIENTES_CLAVE_LEADS=solo-local-clave-de-servicio-leads-0001
# La clave de "leads" en CLAVES_SERVICIO del orquestador (credencial ante el gateway).
ORQUESTADOR_CLAVE_SERVICIO=clave-gateway-leads-local-0000000000001
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
# Tópicos por defecto: crm.comandos, crm.eventos y crm.dlt (KAFKA_TOPICO_COMANDOS / _EVENTOS / _DLT).
```

```bash
uvicorn app.main:app --port 8000 --reload
```

Las tablas se crean solas en el schema `leads` (el usuario `leads` lo tiene como `search_path`).

### IAllmFormulario (ia-resumen)

```dotenv
CLIENTES_URL=http://127.0.0.1:8080/api
VEHICULOS_URL=http://127.0.0.1:8080/api
# La clave de "ia-resumen" en CLAVES_SERVICIO del orquestador (credencial ante el gateway).
ORQUESTADOR_CLAVE_SERVICIO=clave-gateway-ia-resumen-local-0000000001
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

### CalificacionMicroservice

```dotenv
SPRING_DATASOURCE_URL=jdbc:postgresql://127.0.0.1:5432/postgres
SPRING_DATASOURCE_USERNAME=calificacion
SPRING_DATASOURCE_PASSWORD=calificacion
SPRING_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
CLIENTES_SERVICE_URL=http://127.0.0.1:8080/api
VEHICULOS_SERVICE_URL=http://127.0.0.1:8080/api
CLIENTES_CLAVE_CALIFICACION=solo-local-clave-de-servicio-calificacion-0001
# La clave de "calificacion" en CLAVES_SERVICIO del orquestador (credencial ante el gateway).
ORQUESTADOR_CLAVE_SERVICIO=clave-gateway-calificacion-local-00000001
```

```bash
./mvnw spring-boot:run          # PowerShell: .\mvnw.cmd spring-boot:run
```

### AuthMicroservice

Login de los operadores (telemarketers y admins). Python/FastAPI, en el schema `auth`.

```dotenv
DATABASE_URL=postgresql://auth:auth@127.0.0.1:5432/postgres
DB_SCHEMA=auth
# El MISMO secreto que JWT_SECRET del orquestador. 32+ caracteres o el servicio no arranca.
JWT_SECRET=local-jwt-secret-solo-desarrollo-0123456789abcdef
# Crea este ADMIN al arrancar si no existe. Usar un dominio válido (admin@crm.test lo rechaza el login).
SEED_ADMIN_EMAIL=admin@example.com
SEED_ADMIN_PASSWORD=admin-local-123
FRONTEND_LIST=["http://localhost:5173"]
```

```bash
uvicorn app.main:app --port 8086 --reload
```

Después, el login se hace **por el orquestador** (`POST http://127.0.0.1:8080/api/auth/login`). Ver [06-autenticacion.md](06-autenticacion.md).

### VehiculosMicroservice

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
./mvnw -pl vehiculos-service spring-boot:run      # PowerShell: .\mvnw.cmd ...
```

El seed carga 10 unidades (8 disponibles), todas con patente de prueba. El Toyota Corolla `a0000000-0000-4000-8000-000000000001` (patente `AAA111`, de $24.500.000) sirve como `vehiculoId` de prueba para los leads y para las multas: `AAA111` tiene actas reales en la ANSV. Si tu base local ya tenía el seed de antes, al arrancar Flyway aplica la V2 y el seed completa las patentes que falten.

### ClientMicroservice (clientes)

Es el servicio real; reemplaza al simulador. Su perfil `local` (el que usa por defecto) ya trae claves ficticias y espera una base propia (`clientes_db`) y Kafka en el 29092, así que alcanza con redirigirlo a nuestra infra:

```dotenv
SPRING_DATASOURCE_URL=jdbc:postgresql://127.0.0.1:5432/postgres
KAFKA_PUERTO_HOST=9092
```

Usuario y contraseña de la base (`clientes` / `clientes`), las claves de servicio y las del CUIL ya vienen por defecto en el perfil `local`. Por eso las claves de leads y calificacion de arriba son las que trae el perfil: `solo-local-clave-de-servicio-leads-0001` y `solo-local-clave-de-servicio-calificacion-0001`. Exige claves de **32 caracteres o más y distintas entre sí**; las `clave-local-*` de antes eran más cortas y el servicio no arrancaba.

```bash
./mvnw -pl contracts -am install -DskipTests       # una sola vez (y cuando cambie contracts)
./mvnw -pl clientes-service spring-boot:run        # PowerShell: .\mvnw.cmd ...
```

El comando del README de clientes (`-pl clientes-service -am spring-boot:run`) **falla**: con `-am` Maven también intenta correr `spring-boot:run` sobre el POM padre ("Unable to find a suitable main class"). Por eso se instala `contracts` aparte y se corre solo el módulo del servicio.

Flyway crea las tablas en el schema `clientes`. Swagger en <http://localhost:8082/swagger-ui.html> y salud en `/actuator/health`.

Declara los tres tópicos del ecosistema (`crm.comandos`, `crm.eventos` y `crm.dlt`) y los crea si no existen. Escucha `crm.comandos` y atiende el comando `RegistrarCalificacion` (con `destino: clientes`); sus eventos `ClienteActualizado` y `ClienteAnonimizado` salen por `crm.eventos`.

Si tu volumen de Kafka tiene mensajes viejos que clientes no puede leer, terminan en `crm.dlt` con un WARN: es normal.

### InfraccionScrapper (multas)

El servicio que consulta la ANSV por patente: atiende `ConsultarMultas` de `crm.comandos` y responde en `crm.eventos` (docs/08 §7). Node, **solo con pnpm**.

```dotenv
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092
# Vacías para el Kafka local (el .env.example trae lo de Aiven):
KAFKA_SECURITY_PROTOCOL=
KAFKA_SSL_CAFILE=
PORT=8087
```

```bash
pnpm install
pnpm exec playwright install chromium   # una sola vez: el navegador que usa para consultar la ANSV
pnpm start
```

`pnpm start` no lee el `.env` solo: exportar las variables antes en la terminal (o dejar los valores por defecto, que ya apuntan a `localhost:9092` y sin seguridad). Para consultar una patente sin Kafka: `pnpm exec node src/cli.js AAA111`.

### MonitorMicroservice

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
ORQUESTADOR_SERVICE_URL=http://127.0.0.1:8080
IA_RESUMEN_SERVICE_URL=http://127.0.0.1:8001
AUTH_SERVICE_URL=http://127.0.0.1:8086
MULTAS_SERVICE_URL=http://127.0.0.1:8087
```

```bash
./mvnw spring-boot:run
```

Sin estas variables usa H2 en memoria y también funciona.

### DDA2 (front)

```dotenv
# Todo el front (leads, clientes, calificaciones, vehiculos y auth) pasa por el orquestador.
VITE_API_URL=http://localhost:8080/api
# Solo el Health Dashboard (/monitor) llama directo al servicio monitor.
VITE_MONITOR_API_URL=http://localhost:8085
```

La cola (`/cola`) pide login: usar `admin@example.com` / `admin-local-123` (el admin que crea AuthMicroservice con su seed). El orquestador ya permite el origen `http://localhost:5173` en CORS. El sondeo de respaldo de la página Monitor sigue yendo directo a los servicios (ver [03-despliegue-render.md §4](03-despliegue-render.md#nota-sondeo-de-respaldo-del-front)).

## 3. Probar el flujo completo

0. Iniciar sesión como operador (el preformulario no lo necesita, todo lo demás sí):

   ```bash
   curl -X POST http://127.0.0.1:8080/api/auth/login -H "Content-Type: application/json" \
        -d '{"email":"admin@example.com","password":"admin-local-123"}'
   # → {"access_token":"...","refresh_token":"...","token_type":"bearer"}
   ```

   En los comandos siguientes, los que piden login llevan `-H "Authorization: Bearer <access_token>"`. El token dura 15 minutos; se renueva con `POST /api/auth/refresh`.
1. Enviar un preformulario por el orquestador (**público**, sin token):

   ```bash
   curl -X POST http://127.0.0.1:8080/api/leads -H "Content-Type: application/json" -H "X-Correlation-Id: prueba-1" -d @preformulario.json
   ```

   Usar un `vehiculoId` existente (ej. el Corolla del seed).
2. Mirar en kafka-ui (<http://localhost:8090>) los mensajes de `crm.eventos` (`PreformularioCompletado`, `LeadCalificado`, `ResumenGenerado`) y de `crm.comandos` (`CalificarLead`, `RegistrarCalificacion`, `ActualizarCalificacion`, `GenerarResumen`, `GuardarResumen`, cada uno con su `destino`). Contrato: [08-contrato-tres-topicos.md](08-contrato-tres-topicos.md).
3. Ver las llamadas que pasaron por el orquestador:

   ```bash
   curl -H "Authorization: Bearer <access_token>" "http://127.0.0.1:8080/api/orquestador/servicios?ventana_minutos=60"
   ```

   O directo en la base: `select * from orquestador.llamada_servicio order by id;`

4. Ver el flujo del lead y el historial del cliente (el `leadId` lo devuelve el `POST`; el `clienteId` está en `GET /api/leads/{leadId}`):

   ```bash
   curl -H "Authorization: Bearer <access_token>" "http://127.0.0.1:8080/api/orquestador/flujos?leadId=<leadId>"
   curl -H "Authorization: Bearer <access_token>" "http://127.0.0.1:8080/api/clientes/<clienteId>/historial"
   ```

**Resultado esperado** (verificado el 2026-11-14 con los seis servicios nativos):

| Momento | Flujo en el orquestador | Otros |
|---|---|---|
| Unos segundos después | `RESUMEN_PENDIENTE`, con `CALIFICAR_LEAD` en `OK` (~1 s) | El lead queda `CALIFICADO` y el historial de clientes tiene la calificación `AUTOMATICA` |
| Sin `OLLAMA_API_KEY` | A los `FLUJO_TIMEOUT_RESUMEN_SEGUNDOS`: `EXPIRADO`, con `GENERAR_RESUMEN` en `TIMEOUT` y el motivo en `ultimoError` | ia-resumen loguea `La variable OLLAMA_API_KEY no está configurada` |
| Llega `ResumenGenerado` (aunque sea tarde) | `COMPLETADO`: el flujo sale de `EXPIRADO` | leads guarda el resumen |
| `POST /api/calificaciones/{leadId}/correcciones` | Paso `CORRECCION_CALIFICACION` `OK`, sin cambiar el estado | Historial con una entrada `MANUAL` más |

5. Consultar las multas de una unidad (la del Corolla tiene la patente `AAA111`, con actas reales):

   ```bash
   curl -X POST -H "Authorization: Bearer <access_token>" "http://127.0.0.1:8080/api/orquestador/multas/a0000000-0000-4000-8000-000000000001"
   curl -H "Authorization: Bearer <access_token>" "http://127.0.0.1:8080/api/orquestador/flujos?leadId=a0000000-0000-4000-8000-000000000001&tipo=CONSULTA_MULTAS_VEHICULO"
   curl -H "X-Api-Key: clave-local-vehiculos-de-32-caracteres-o-mas" "http://127.0.0.1:8083/vehiculos/a0000000-0000-4000-8000-000000000001/multas"
   ```

   El flujo pasa por `CONSULTANDO_MULTAS` y `ACTUALIZANDO_VEHICULO` hasta `COMPLETADO` (unos segundos: el scraper abre Chromium y consulta la ANSV). El detalle de las actas queda en vehiculos, no en el orquestador.

Para ver el cierre sin Ollama, publicar a mano un evento `ResumenGenerado` en `crm.eventos`, con sobre y `payload: {leadId, resumen}` (clave del mensaje = `leadId`), con el mismo `kafka-console-producer` de la sección siguiente.

Al arrancar contra un Kafka recién creado, el orquestador loguea errores `Topic ... not found in cluster metadata` y `GroupCoordinatorNotAvailableError`: es ruido del arranque y se recupera solo.

### Simular un servicio que no está levantado

Para probar un tramo sin tener todos los servicios, se puede publicar a mano el evento que publicaría el servicio faltante. Por ejemplo, el `LeadCalificado` de calificacion (siempre con sobre, en `crm.eventos`):

```bash
echo '<leadId>|{"eventId":"<uuid>","eventType":"LeadCalificado","version":1,"occurredAt":"2026-11-15T12:00:00Z","timestamp":"2026-11-15T12:00:00Z","correlationId":"<uuid>","payload":{"leadId":"<leadId>","clienteId":"<clienteId>","puntos":55,"nivel":"POTABLE","origen":"AUTOMATICA","fecha":"2026-11-15T12:00:00","motivos":[]}}' \
  | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic crm.eventos \
      --property parse.key=true --property "key.separator=|"
```

El orquestador ordena entonces `RegistrarCalificacion`, `ActualizarCalificacion` y `GenerarResumen` en `crm.comandos`.

En Git Bash anteponer `MSYS_NO_PATHCONV=1` a los comandos `docker compose exec`, porque si no Git Bash reescribe las rutas `/opt/...`.

## 4. Problemas conocidos

| Síntoma | Causa | Solución |
|---|---|---|
| El orquestador informa la base `DOWN` con `ProactorEventLoop` en el log | Se levantó con `uvicorn` en Windows | Usar `python -m app` |
| leads no arranca: `No module named 'psycopg2'` | `DATABASE_URL` con `postgresql://` y SQLAlchemy 2.0 instalado | Usar `postgresql+psycopg://` |
| Cada llamada entre servicios tarda ~2 s de más | `localhost` en Windows | Usar `127.0.0.1` |
| `port is already allocated` al levantar el compose | Otro Postgres o Kafka en 5432/9092 (por ejemplo el compose de leads) | Apagar el otro, o `POSTGRES_PUERTO=5433` |
| clientes no arranca o responde `401 CLAVE_SERVICIO_INVALIDA` | Claves de servicio de menos de 32 caracteres, iguales entre sí, o distintas entre clientes y quien lo llama | Usar las del perfil `local` (ver sección de clientes) en los `.env` de leads y calificacion |
| El orquestador no arranca: `Falta JWT_SECRET` | La autenticación está habilitada por defecto y no hay secreto | Definir `JWT_SECRET` (igual al de auth), o `AUTH_HABILITADA=false` solo para desarrollo |
| `401 NO_AUTENTICADO` / `TOKEN_EXPIRADO` en una ruta que antes andaba | Ahora las rutas del operador piden login | Hacer login (paso 0 de la sección 3) y mandar el `Authorization: Bearer` |
| `401 CLAVE_SERVICIO_INVALIDA` de un servicio al llamar por el orquestador | Su `ORQUESTADOR_CLAVE_SERVICIO` no coincide con su entrada en `CLAVES_SERVICIO` | Revisar que sea la misma clave, de 32+ caracteres |
| `422` al hacer login con un email que existe en la base | `email-validator` rechaza dominios reservados como `.test` o `.local` | Usar un dominio válido (`@example.com`) |
| Multas `FALLIDO` con "no tiene patente" | La unidad no tiene patente cargada | Cargarla con `PUT /api/vehiculos/{id}` (`{"patente":"AB123CD"}`) o volver a correr el seed |
| El scraper falla con `Executable doesn't exist` | Falta el navegador de Playwright | `pnpm exec playwright install chromium` |
| Los schemas no existen | El volumen de Postgres ya tenía datos de antes del script de inicialización | `docker compose down -v` y volver a levantar |
