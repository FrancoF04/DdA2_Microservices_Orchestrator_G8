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
| multas (propuesto, en desarrollo) | 8087 |
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
# Autenticación (docs/06-autenticacion.md): sin JWT_SECRET el orquestador NO arranca.
# El mismo JWT_SECRET que AuthMicroservice. Claves de servicio: 32+ caracteres y distintas.
JWT_SECRET=local-jwt-secret-solo-desarrollo-0123456789abcdef
CLAVES_SERVICIO=leads=clave-gateway-leads-local-0000000000001,calificacion=clave-gateway-calificacion-local-00000001,ia-resumen=clave-gateway-ia-resumen-local-0000000001
# Para trabajar sin login (solo desarrollo): AUTH_HABILITADA=false
# Tanda A: true = el orquestador publica resumen-solicitado (y leads con LEADS_PUBLICA_RESUMEN=false).
# Por defecto false: leads sigue publicándolo y el orquestador solo observa.
ORQUESTADOR_PUBLICA_RESUMEN=false
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
# true (por defecto): leads publica resumen-solicitado. false: lo publica el orquestador.
LEADS_PUBLICA_RESUMEN=true
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

El seed carga 8 unidades. Por ejemplo, el Toyota Corolla `a0000000-0000-4000-8000-000000000001`, de $24.500.000, sirve como `vehiculoId` de prueba.

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

Crea sus propios tópicos (`cliente-actualizado`, `cliente-anonimizado` y la DLT `lead-calificado.clientes.DLT`) y **no crea `lead-calificado`**, que es de calificacion (con el orquestador levantado, que lo escucha, se crea solo).

Al arrancar lee `lead-calificado` desde el principio. Los mensajes viejos que tengas en el volumen de Kafka pueden terminar en la DLT con un WARN: es normal. Si usás calificacion **sin** el fix de `fecha` (rama `fix/fecha-lead-calificado-iso`, PR pendiente), sus mensajes reales **también van a la DLT** y el historial de clientes queda vacío (ver [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md)).

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
2. Mirar en kafka-ui (<http://localhost:8090>) los mensajes de `preformulario-completado`, `lead-calificado`, `resumen-solicitado` y `resumen-generado`.
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
| Llega `resumen-generado` (aunque sea tarde) | `COMPLETADO`: el flujo sale de `EXPIRADO` | leads guarda el resumen |
| `POST /api/calificaciones/{leadId}/correcciones` | Paso `CORRECCION_CALIFICACION` `OK`, sin cambiar el estado | Historial con una entrada `MANUAL` más |

Para ver el cierre sin Ollama, publicar a mano un `resumen-generado` con sobre y `payload: {leadId, resumen}` (clave del mensaje = `leadId`), con el mismo `kafka-console-producer` de la sección siguiente.

Al arrancar contra un Kafka recién creado, el orquestador loguea errores `Topic ... not found in cluster metadata` y `GroupCoordinatorNotAvailableError`: es ruido del arranque y se recupera solo.

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
| clientes no arranca o responde `401 CLAVE_SERVICIO_INVALIDA` | Claves de servicio de menos de 32 caracteres, iguales entre sí, o distintas entre clientes y quien lo llama | Usar las del perfil `local` (ver sección de clientes) en los `.env` de leads y calificacion |
| El orquestador no arranca: `Falta JWT_SECRET` | La autenticación está habilitada por defecto y no hay secreto | Definir `JWT_SECRET` (igual al de auth), o `AUTH_HABILITADA=false` solo para desarrollo |
| `401 NO_AUTENTICADO` / `TOKEN_EXPIRADO` en una ruta que antes andaba | Ahora las rutas del operador piden login | Hacer login (paso 0 de la sección 3) y mandar el `Authorization: Bearer` |
| `401 CLAVE_SERVICIO_INVALIDA` de un servicio al llamar por el orquestador | Su `ORQUESTADOR_CLAVE_SERVICIO` no coincide con su entrada en `CLAVES_SERVICIO` | Revisar que sea la misma clave, de 32+ caracteres |
| `422` al hacer login con un email que existe en la base | `email-validator` rechaza dominios reservados como `.test` o `.local` | Usar un dominio válido (`@example.com`) |
| Los schemas no existen | El volumen de Postgres ya tenía datos de antes del script de inicialización | `docker compose down -v` y volver a levantar |
