# Cambios pendientes en otros repos

Lista de lo que hay que cambiar en cada repo del ecosistema para que todo encaje con el orquestador. Los hace el equipo, con una PR en cada repo, y se documentan en ese repo.

**Ninguno cambia la lógica de dominio de los servicios:** son de integración (tópicos, headers, salud, despliegue).

| Prioridad | Significado |
|---|---|
| 🔴 **Bloqueante** | Sin esto no se puede avanzar a la fase indicada. |
| 🟡 **Recomendado** | Funciona sin esto, pero se pierde trazabilidad o hay un riesgo conocido. |
| ⚪ **Opcional** | Mejora menor. |

## Cambio transversal: pasar a 3 tópicos de Kafka

**Por qué:** el plan free de Aiven permite 5 tópicos y ya están todos en uso. La orquestación completa necesita que los servicios se comuniquen por el orquestador, no entre ellos. Detalle en [02-propuesta-orquestador.md §5.2](02-propuesta-orquestador.md).

| Tópico nuevo | Reemplaza a |
|---|---|
| `crm.comandos` (orquestador → servicios) | `preformulario-completado` (lado consumidor), `resumen-solicitado`, el consumo de `lead-calificado` por clientes (`RegistrarCalificacion`), y el nuevo `ConsultarMultas` |
| `crm.eventos` (servicios → orquestador) | `preformulario-completado` (lado productor), `lead-calificado`, `resumen-generado`, `cliente-actualizado`, `cliente-anonimizado`, y el nuevo `MultasConsultadas` |
| `crm.dlt` | `preformulario-completado-dlt`, `lead-calificado.clientes.DLT` |

Qué cambia en cada servicio que usa Kafka:

1. **Publicar** sus hechos en `crm.eventos`, con el sobre común y un `eventType` (ver abajo).
2. **Escuchar** solo `crm.comandos` e **ignorar** los comandos cuyo `destino` no sea él.
3. Mandar los mensajes que fallan a `crm.dlt`.

Sobre común (el que ya usan leads e ia-resumen, más `version` y `destino`):

```json
{
  "eventId": "uuid",
  "eventType": "CalificarLead",
  "version": 1,
  "occurredAt": "2026-11-14T12:00:00Z",
  "timestamp": "2026-11-14T12:00:00Z",
  "correlationId": "uuid",
  "destino": "calificacion",
  "payload": { "leadId": "uuid", "...": "..." }
}
```

`destino` va solo en los comandos. La clave del mensaje sigue siendo el `leadId` (o el `vehiculoId` en el flujo de multas).

**Cómo se hace el pase:**

- Se prototipa entero en local (Kafka del compose, sin límite de tópicos).
- En Aiven se aplica en un cambio **coordinado**: los 5 tópicos viejos y los 3 nuevos no pueden convivir.
- El orquestador publica los nombres de los tópicos y los `eventType` como contrato antes de que cada repo empiece su PR.

**Estado:** pendiente. Es la fase de orquestación completa, después de la 1b.

## Hechos

| Fecha | Repos | Cambio |
|---|---|---|
| 2026-11-15 | LeadsMicroservice (rama `feat/resumen-lo-publica-el-orquestador`, sin PR todavía) | `LEADS_PUBLICA_RESUMEN` (por defecto `true`, igual que hoy): con `false` leads ya no publica `resumen-solicitado` porque lo hace el orquestador. Además `GET /health` (`UP`, o 503 `DOWN` si la base no responde). |
| 2026-11-15 | MonitorMicroservice (rama `feat/objetivos-orquestador-auth-ia`, sin PR todavía) | Suma como objetivos al orquestador (`/health`), ia-resumen (`/openapi.json`) y auth (`/health`). Variables `ORQUESTADOR_SERVICE_URL`, `IA_RESUMEN_SERVICE_URL`, `AUTH_SERVICE_URL`. |
| 2026-11-14 | CalificacionMicroservice (#6), VehiculosMicroservice (#19), MonitorMicroservice | Maven Wrapper con Maven 3.9.16: ya no hace falta instalar Maven para correrlos. |
| 2026-11-14 | CalificacionMicroservice (rama `fix/fecha-lead-calificado-iso`, PR pendiente) | `fecha` de `lead-calificado` como texto ISO-8601 (antes array). Con esto el historial de clientes se llena en el flujo real. |
| 2026-11-15 | CalificacionMicroservice (misma rama, PR pendiente) | `spring-boot-starter-actuator` y `GET /actuator/health`: el monitor deja de ver a calificacion como `DOWN`. |
| 2026-11-15 | CalificacionMicroservice (#7) | Mergeadas la `fecha` ISO-8601 y `/actuator/health`. |
| 2026-11-15 | AuthMicroservice (rama `fix/seguridad-tokens`, sin PR todavía) | `/auth/refresh` falla si el usuario está inactivo; desactivar un usuario revoca sus refresh tokens; `JWT_SECRET` obligatorio (32+ caracteres); claim `email`; access token de 15 min; puerto 8086; dependencias fijadas. |
| 2026-11-15 | LeadsMicroservice, IAllmFormulario, CalificacionMicroservice (ramas `feat/identidad-de-servicio-orquestador`, sin PR todavía) | Se identifican ante el orquestador con `X-Origen-Servicio` + `X-Clave-Servicio` (variable `ORQUESTADOR_CLAVE_SERVICIO`, opcional si llaman directo). calificacion además toma el autor de la corrección manual de `X-Usuario`. |
| 2026-11-14 | ClientMicroservice (#8) | Maven Wrapper con Maven 3.9.16 (PR abierta; con esto los cuatro repos Java lo tienen). |

## Por repo

### LeadsMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Orquestación completa | Pasar a `crm.comandos` / `crm.eventos` (ver arriba). Deja de publicar `resumen-solicitado`: ese paso lo decide el orquestador. | Que el flujo viva en el orquestador. |
| ✅ | `GET /health` (hecho, rama `feat/resumen-lo-publica-el-orquestador`). | El monitor ya lo usa; antes caía a `/openapi.json`. |
| 🟡 | Mandar `X-Origen-Servicio: leads` en la llamada a clientes. | Que el orquestador sepa quién llama (hoy figura `desconocido`). |
| ⚪ | En `.env.example`: usar `postgresql+psycopg://` en `DATABASE_URL` y sacar `CLIENT_MICROSERVICE`, `VEHICLE_MICROSERVICE` y `BCRA_API`, que no se usan. | Evitar confusiones al levantarlo. |
| ⚪ | Fijar versiones en `requirements.txt`. | Hoy instala la última de cada paquete. |

### CalificacionMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| ✅ Hecho (PR pendiente) | Publicar `fecha` de `lead-calificado` como **texto ISO-8601**, no como array. Rama `fix/fecha-lead-calificado-iso` (`@JsonFormat(shape = STRING)` + test de regresión), verificada de punta a punta: el historial de clientes se llena y la DLT queda vacía. Problema original: Hoy `KafkaTemplate` usa el `JsonSerializer` de Spring Kafka, que escribe `LocalDateTime` como `[2026,11,14,18,56,6,49848900]`. Posible arreglo (a validar por el responsable): construir el `JsonSerializer` con un `ObjectMapper` que tenga `WRITE_DATES_AS_TIMESTAMPS` desactivado (por ejemplo el de Spring), o publicar el JSON ya serializado como `String`. | **Verificado en local (2026-11-14):** clientes rechaza el mensaje (`'fecha' no es una fecha ISO-8601`) y lo manda a `lead-calificado.clientes.DLT`. Mientras tanto el historial de clientes queda **vacío** en el flujo real. (Alternativa: que clientes también acepte el array; conviene corregir el productor, que es el que se aparta del contrato.) |
| 🔴 Orquestación completa | Pasar a `crm.comandos` / `crm.eventos`. Consumir el comando `CalificarLead` en vez de `preformulario-completado`. | Ídem. |
| 🟡 | Publicar `lead-calificado` **con el sobre común** (hoy va como JSON plano). | Hoy se pierde el `correlationId` en ese paso: no se puede seguir un flujo de punta a punta con un solo ID. Verificado en local. |
| ✅ Hecho (PR pendiente) | Faltaba Spring Boot Actuator: `GET /actuator/health` respondía `500` (lo devolvía su `GlobalExceptionHandler` como "No static resource") y el monitor, que sondea esa ruta (`TipoVerificacion.ACTUATOR_HEALTH`), mostraba a calificacion como `DOWN` ("HTTP 500") aunque funcionaba. Se agregó `spring-boot-starter-actuator` (versión del parent) y se expone solo `health`, en la misma rama `fix/fecha-lead-calificado-iso` (commit aparte). | Verificado en local (2026-11-15) con el monitor corriendo: `UP`; y con la base caída, `503 DOWN` y el monitor lo refleja. Sirve también para healthchecks de Docker/Render. |
| 🟡 | `motivos` como objetos `{regla, puntos, descripcion}` en vez de texto formateado. | Es lo que dice el contrato y lo que necesita el historial de clientes. |
| 🟡 | Mandar `X-Origen-Servicio: calificacion` en las llamadas a vehiculos y clientes. | Trazabilidad. |
| 🟡 | Reenviar el `correlationId` del evento recibido como header `X-Correlation-Id` en las llamadas REST a vehiculos y clientes. | Verificado en local: hoy cada llamada sale sin ese header, el orquestador genera un ID nuevo y se pierde el hilo del lead. |
| ⚪ | Fijar las imágenes del `Dockerfile` con versión exacta y digest. | Builds reproducibles en Render y cumplir la regla de 3 días. Solo afecta al deploy, no al desarrollo local. |

### IAllmFormulario (ia-resumen)

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Ecosistema en Docker / nube | Agregar un `Dockerfile`. | Es el único servicio sin imagen. |
| 🔴 Orquestación completa | Consumir el comando `GenerarResumen` de `crm.comandos` y publicar `ResumenGenerado` en `crm.eventos`. | Ídem. |
| 🟡 | Si no consigue el cliente o el vehículo, generar el resumen igual con lo que tenga (o publicar un error). | Verificado en local: si vehiculos no responde, el consumer revienta con un error de validación y el resumen no se genera nunca, sin avisar a nadie. |
| 🟡 | Mandar `X-Origen-Servicio: ia-resumen` en las llamadas. | Trazabilidad. |
| 🟡 | Reenviar el `correlationId` del evento `resumen-solicitado` como header `X-Correlation-Id` en las llamadas a clientes y vehiculos. | Ídem calificacion: hoy se pierde el hilo del lead. |
| ⚪ | Mover `pytest` a un `requirements-dev.txt`. | Hoy se instala en producción. |

### VehiculosMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Flujo de multas | Agregar el campo `patente` (migración de Flyway, la hace el responsable). | El scraper de multas consulta por patente. |
| 🔴 Flujo de multas | Recibir el resultado del scraper: `PUT /vehiculos/{id}/multas` o comando por `crm.comandos`, a definir. | Que vehiculos decida qué hacer con las multas. |
| ⚪ | Fijar las imágenes del `Dockerfile` con versión exacta y digest. | Ídem calificacion. |

### MonitorMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| ✅ | Objetivos **orquestador**, **ia-resumen** y **auth** (hecho, rama `feat/objetivos-orquestador-auth-ia`). Quedan clientes real (ya figura) y multas cuando exista. | El orquestador reporta `"status": "DOWN"` en su `/health` si la base o Kafka están caídos, y el monitor lo marca DOWN. |
| ⚪ | Usar Postgres en vez de H2 en memoria (ya lo soporta por variables). | Hoy pierde el historial en cada reinicio. |

**No cambiar:** el monitor tiene que seguir sondeando a cada servicio **directo**, no a través del orquestador.

### DDA2 (front)

| Prioridad | Cambio | Para qué |
|---|---|---|
| ✅ Hecho (rama `feat/login-operador`) | El front pasa todo por el orquestador (`VITE_API_URL`) y tiene login del operador: `axiosClient` manda `Authorization: Bearer`, renueva con `POST /api/auth/refresh` ante `401 TOKEN_EXPIRADO` y reintenta, vuelve al login si el refresh falla o el token fue revocado, y cierra sesión con `POST /api/auth/logout`. La cola (`/cola`) pide login; el preformulario y el catálogo de vehículos no. | Ver [06-autenticacion.md](06-autenticacion.md). Falta probarlo en el navegador. |
| 🟡 | Rehacer el sondeo de respaldo de la página Monitor (`src/api/monitorClient.js`). | Con las URLs nuevas deja de funcionar. Opciones en [03-despliegue-render.md §4](03-despliegue-render.md#nota-sondeo-de-respaldo-del-front). |
| ⚪ | Si se despliega el orquestador, `/monitor` (que sigue llamando directo al servicio monitor con `VITE_MONITOR_API_URL`) puede pasar a `/api/monitor` y pedir login. | Hoy el Health Dashboard no exige sesión. |

### ClientMicroservice (clientes)

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Orquestación completa | Pasar a `crm.comandos` / `crm.eventos` / `crm.dlt`: publicar `ClienteActualizado` y `ClienteAnonimizado` en `crm.eventos`; consumir el comando `RegistrarCalificacion` en vez de `lead-calificado`; mandar sus fallidos a `crm.dlt` en vez de `lead-calificado.clientes.DLT`. | Hoy crea 3 tópicos propios que no entran en el plan free de Aiven (límite de 5). |
| ✅ Hecho en calificacion (PR pendiente) | Ver el cambio de `fecha` en CalificacionMicroservice. Opcional de este lado: aceptar también el array `[año, mes, día, ...]` en `LeadCalificadoTraductor`. | El historial de calificaciones no se llena. |
| 🟡 | Corregir el comando del README: `mvnw -pl clientes-service -am spring-boot:run` **falla** porque con `-am` Maven corre `spring-boot:run` también sobre el POM padre. Funciona con `mvnw -pl contracts -am install -DskipTests` (una vez) y luego `mvnw -pl clientes-service spring-boot:run`. | Que se pueda levantar siguiendo el README. |
| 🟡 | Perfil `local` apuntando a la infra compartida (un Postgres con schema por servicio y Kafka en 9092), o un perfil nuevo. Hoy espera la base `clientes_db` y Kafka en el 29092 del compose del propio repo. | Hoy hay que pisarlo con `SPRING_DATASOURCE_URL` y `KAFKA_PUERTO_HOST` (ver [04](04-desarrollo-local.md)). |
| 🟡 | Mandar `X-Origen-Servicio: clientes` si llegara a llamar a otro servicio. Hoy no llama a ninguno. | Trazabilidad. |
| ✅ | `GET /clientes/{id}/historial` y las escrituras del personal ya están protegidas: el orquestador exige login y le manda `X-Usuario` (ver [06](06-autenticacion.md)). | Seguridad. |
| ⚪ | Que alguien consuma `cliente-actualizado` y `cliente-anonimizado` (leads los necesita para mantener sus copias). | Hoy nadie los lee. |

### InfraccionScrapper (multas)

Hoy es una librería y CLI de Node (consulta la ANSV por patente). Para ser el servicio **multas** del ecosistema le falta:

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Flujo de multas | Un proceso que **consuma el comando `ConsultarMultas`** de `crm.comandos` (ignorando los que no tengan `destino: multas`) y **publique `MultasConsultadas`** (o `ConsultaMultasFallida`) en `crm.eventos`, con el sobre común, el `correlationId` y la clave `vehiculoId`. Aiven y Kafka local con SASL/SSL como el resto. | Es el canal decidido (asincrónico por Kafka). |
| 🔴 Flujo de multas | **Concurrencia limitada** (arrancar en 1 consulta a la vez) y manejo de los tres errores existentes: `ErrorSitioNoDisponible` (reintentable), `ErrorEstructuraNoReconocida` (no reintentable, alerta) y patente sin multas (resultado válido). | No saturar ni que nos bloqueen el sitio de la ANSV. |
| 🔴 Ecosistema en Docker / nube | `Dockerfile` basado en la imagen oficial de Playwright (trae Chromium), con usuario no root y versión fijada con digest. | Es el único repo sin imagen; Chromium no entra en un Dockerfile común. Ojo: la memoria del plan free de Render. |
| 🟡 | Un `GET /health` (o un endpoint mínimo) para el monitor. Aunque el canal sea Kafka, el monitor sondea por HTTP. | Que el monitor y el orquestador vean si está vivo. |
| 🟡 | Mantener el log sin datos personales (patente, actas, importes), como ya hace la CLI. | Ley 25.326. |
| 🟡 | `pnpm` como único gestor (hay un `pnpm-workspace.yaml` en camino): `package-lock.json` y los comandos `npm`/`npx` del README pasan a `pnpm`. | Coherencia con el equipo. |
| ⚪ | Una caché corta por patente. | Menos pedidos a la ANSV. |

Del lado de **vehiculos** (ya en su sección): el campo `patente` y el destino del resultado.

### AuthMicroservice (auth)

Ya existe y está integrado (ver [06-autenticacion.md](06-autenticacion.md)). Lo hecho al integrarlo está en "Hechos". Queda:

| Prioridad | Cambio | Para qué |
|---|---|---|
| ✅ Hecho (PR pendiente) | **Límite de intentos de login** por email: 5 fallos seguidos → `429` con `Retry-After` durante 5 minutos (configurable con `LOGIN_*`). Pendiente, si hiciera falta: que sobreviva a reinicios y a varias réplicas (hoy vive en memoria). | Frena la fuerza bruta contra una cuenta. |
| 🟡 | Detectar el **reuso de un refresh token** y cerrar toda la sesión del usuario. | Un refresh robado y usado tras la rotación no se nota. |
| 🟡 | `Dockerfile` con usuario no root, versión de la imagen fijada con digest y `HEALTHCHECK` contra `/health`; `/health` que verifique la base. | Es el único servicio Python sin esto; para el pase a la nube. |
| 🟡 | Agregar auth como objetivo del **monitor** (`/health`). | Hoy no lo vigila. |
| ⚪ | Migraciones (Alembic) en vez de `create_all`, como el orquestador. | Cambios de esquema sin perder datos. |
| ⚪ | Pasar a **RS256**: auth firma con la clave privada y el orquestador solo necesita la pública. | Hoy quien tenga `JWT_SECRET` puede firmar tokens. |
| ⚪ | Que el orquestador revoque por usuario cuando ve el `PATCH /api/auth/usuarios/{id}` con `activo: false`. | Hoy el access token de un usuario desactivado vale hasta 15 min. |
| ⚪ | Validar que `SEED_ADMIN_EMAIL` sea un email válido para el login (`.test` y `.local` los rechaza `email-validator`). | Evita crear un admin que no puede entrar. |

### Servicios nuevos

Al crearlos conviene que nazcan ya con:

- `GET /health` (o Actuator) y un `Dockerfile`.
- Kafka con el esquema de 3 tópicos, si lo usan.
- Errores en formato ProblemDetail y `X-Correlation-Id` propagado.
- Header `X-Origen-Servicio` en las llamadas que hagan.
- Nota: **clientes** ya existe; el simulador de este repo ([dev/clientes_simulado](../dev/clientes_simulado/main.py)) queda solo como opción para probar sin levantar Java.
