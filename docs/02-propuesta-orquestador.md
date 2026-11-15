# Propuesta del orquestador

Basada en el relevamiento de [01-relevamiento-ecosistema.md](01-relevamiento-ecosistema.md). Las referencias `R#` (REST) y `K#` (Kafka) son las de la sección 4 de ese documento.

## 1. Qué es (y qué no es) este servicio

El orquestador cumple dos roles en un mismo servicio:

1. **Puerta de entrada única (rol de API Gateway).** Todo el tráfico HTTP, tanto el del front hacia los servicios (norte-sur) como el de un servicio hacia otro (este-oeste), entra por `/api/...` y el orquestador lo reenvía al servicio dueño. Así cada llamada queda registrada con su origen, destino, código y latencia. Ocupa el lugar del gateway que el documento de arquitectura prevé (AD-10, puerto 8080) y que nunca se desplegó.
2. **Orquestador de flujos** (en la teoría, patrón *Saga*). Sigue cada flujo de negocio de varios pasos (empezando por el registro de un lead) como una máquina de estados persistida, para saber en qué paso está, cuánto tardó cada servicio y desde dónde reanudar si algo se cae.

**No es:**

- **Dueño de datos de negocio.** No guarda leads, clientes ni vehículos. Tampoco cuerpos de request/response: el `POST /leads` lleva el CUIL, y la Ley 25.326 (AD-04) exige que solo lo conozca `clientes`.
- **Un broker.** Kafka ya enruta por tópico. El orquestador no reenvía mensajes de un tópico a otro "porque sí". Publica solamente cuando él decide el próximo paso del flujo.
- **Un lugar para reglas de negocio.** Decide *qué paso sigue*. *Cómo* se califica o cómo se cambia el estado de un lead lo decide cada servicio.

## 2. ¿Le corresponde a un orquestador manejar Kafka?

**Sí, y es la forma habitual.** En la teoría de microservicios hay dos maneras de coordinar una transacción que cruza varios servicios (patrón *Saga*):

| | Coreografía (lo que hay hoy) | Orquestación (lo que se propone) |
|---|---|---|
| Quién decide el próximo paso | Cada servicio reacciona al evento del anterior | Un orquestador central |
| Cómo se ve el estado del flujo | No se ve: hay que reconstruirlo leyendo logs de todos | Está en la base del orquestador |
| Acoplamiento | Cada servicio conoce qué evento escuchar y cuál publicar después (ej. leads publica `resumen-solicitado`) | Los servicios solo responden a comandos o publican sus hechos; el flujo vive en un solo lugar |
| Transporte | Eventos por broker | **Comandos y respuestas, normalmente por broker (Kafka)** y/o REST |

En la saga orquestada "clásica" (Chris Richardson, *microservices.io*), el orquestador **se comunica de forma asincrónica por el broker**: publica comandos y consume respuestas o eventos. Kafka no es un detalle de infraestructura ajeno al orquestador. Es su canal natural para los pasos asincrónicos.

Lo que **no** conviene:

- Que el orquestador se vuelva un "caño inteligente" que recibe todos los eventos y los re-publica en otros tópicos. Eso duplica a Kafka y no aporta nada (principio *smart endpoints, dumb pipes*).
- Que los servicios dejen de usar Kafka y pasen a hablar todo por REST con el orquestador. Se perdería la resiliencia que da el 202 asincrónico (RNF-01: ningún preformulario se pierde aunque calificacion esté caída).

**Conclusión para este proyecto:** el orquestador es un **participante más del bus**, con su propio grupo consumidor (`orquestador`).

- **Fase 1:** escucha todos los tópicos del flujo para registrar el avance.
- **Fase 2:** pasa a publicar los comandos de los pasos que hoy están "escondidos" en otros servicios.

**Objetivo final (decisión del equipo, 2026-11-14): orquestación completa.** Ningún servicio escucha los eventos de otro servicio. Cada uno escucha solo **sus comandos**, que publica el orquestador, y publica **sus respuestas**, que escucha solo el orquestador. Por ejemplo, calificacion deja de escuchar `preformulario-completado` de leads y pasa a escuchar `calificacion.comandos`. Las fases 1 y 2 son el camino de migración para llegar ahí sin romper lo que ya funciona.

| Hoy (coreografía) | Objetivo (orquestación) |
|---|---|
| leads → `preformulario-completado` → calificacion | leads → `leads.eventos` → **orquestador** → `calificacion.comandos` → calificacion |
| calificacion → `lead-calificado` → leads, clientes | calificacion → `calificacion.eventos` → **orquestador** → `leads.comandos` y `clientes.comandos` |
| leads → `resumen-solicitado` → ia-resumen | **orquestador** → `ia-resumen.comandos` → ia-resumen |

Los nombres de los tópicos son una propuesta. Pasar a este esquema exige cambiar qué tópico escucha y en cuál publica cada servicio. Es poco código por servicio, pero toca a todos los repos.

### Riesgo a asumir: punto único de falla

Si el orquestador es la puerta HTTP de todo, cuando se cae se corta todo el tráfico síncrono. Para un TP es aceptable, y es el mismo riesgo que tenía el gateway del documento. Se mitiga así:

- El proxy HTTP es **sin estado**: el estado de los flujos vive en la base, así que se pueden levantar varias réplicas.
- Los pasos asincrónicos siguen en Kafka: si el orquestador se cae a mitad de un flujo, los eventos esperan en el tópico y el consumidor retoma desde su offset.
- El monitor sondea a los servicios **directo, sin pasar por el orquestador** (ver §4.6), para poder distinguir "se cayó el orquestador" de "se cayó leads".

## 3. Cómo se enruta

### 3.1 Convención de rutas

Todas las rutas internas de los servicios ya empiezan con el nombre de su recurso. El orquestador enruta por el **primer segmento después de `/api`** y reenvía el resto **sin modificar**:

| Prefijo en el orquestador | Servicio destino | Ruta que recibe el servicio |
|---|---|---|
| `/api/leads/**` | leads | `/leads/**` |
| `/api/clientes/**` | clientes* | `/clientes/**` |
| `/api/vehiculos/**` | vehiculos | `/vehiculos/**` |
| `/api/calificaciones/**` | calificacion | `/calificaciones/**` |
| `/api/monitor/**` | monitor | `/monitor/**` |
| `/api/resumen`, `/api/resumen-ia` | ia-resumen | `/resumen`, `/resumen-ia` |
| `/api/auth/**` | auth* | `/auth/**` |
| `/api/multas/**` | scraper de multas* | a definir cuando exista |
| `/api/orquestador/**` | el propio orquestador | endpoints propios (§7.3) |

Así se cierra el punto abierto del documento ("¿`/api/leads/leads` o `/api/leads`?"): queda `/api/leads`.

**Ventaja clave: la fase 1 no requiere tocar código de otros repos.** Alcanza con cambiar la URL base de cada cliente HTTP a `http://<orquestador>:8080/api`. Por ejemplo, leads arma `f"{CLIENTES_URL}/clientes"`. Con `CLIENTES_URL=http://orquestador:8080/api` le pega a `/api/clientes`, que el orquestador reenvía a `clientes:/clientes`.

### 3.2 Headers

| Header | Tratamiento |
|---|---|
| `X-Correlation-Id` | Si viene y es válido (`[A-Za-z0-9._-]{1,64}`, mismo criterio que vehiculos) se respeta; si no, se genera un UUID. Se reenvía al destino y se devuelve en la respuesta. |
| `X-Api-Key` | **Fase 1:** pass-through (cada servicio sigue mandando la clave del destino). **Fase 2 (opcional):** cada servicio se autentica contra el orquestador con su propia clave y el orquestador inyecta la del destino. Eso resuelve I10 (ia-resumen sin clave) y centraliza los secretos. |
| `X-Origen-Servicio` | Header nuevo para saber quién llama. En la fase 1 no lo manda nadie, así que el origen se infiere (front por `Origin`, servicios por `User-Agent` / clave). En la fase 2 se pide a cada servicio que lo agregue. |
| `Host`, `Content-Length`, hop-by-hop | Se reescriben o descartan como en cualquier proxy. |

### 3.3 Comportamiento del proxy

- Cliente `httpx.AsyncClient` compartido, con timeouts por servicio configurables (por defecto 5 s, igual que leads e ia-resumen).
- **Sin reintentos automáticos en métodos no idempotentes** (`POST`, `PATCH`): los reintentos los decide quien llama, como ya hace leads con clientes. Para `GET` se puede habilitar 1 reintento ante timeout o conexión rechazada.
- Errores propios del orquestador en `application/problem+json`: `502 SERVICIO_RECHAZO` cuando el destino responde mal o corta la conexión, `503 SERVICIO_NO_DISPONIBLE` cuando no se puede conectar y `504 SERVICIO_TIMEOUT` cuando no responde a tiempo, con `servicio` y `correlationId`. Las respuestas del destino (incluidos sus 4xx/5xx) **se devuelven tal cual**.
- Cada llamada genera una fila en `llamada_servicio` con la ruta, el código y la latencia. Nunca se guarda el cuerpo.
- El multipart de `PUT /vehiculos/{id}/foto` se reenvía en streaming.
## 4. Qué pasa por el orquestador, repo por repo

Leyenda: **F1** = fase 1, solo configuración. **F2** = fase 2, cambio chico en el otro repo. **No** = queda directo.

### 4.1 Frontend — `DDA2`

| Llamada | Hoy | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|---|
| R1 vehiculos | directo a :8083 / Render | `/api/vehiculos/**` | F1 | `VITE_VEHICULOS_API_URL=<orq>/api` |
| R2 leads | directo a :8000 | `/api/leads/**` | F1 | `VITE_LEADS_API_URL=<orq>/api` |
| R3 calificacion | directo a :8084 | `/api/calificaciones/**` | F1 | `VITE_CALIFICACION_API_URL=<orq>/api` |
| R4 clientes* | directo a :8082 | `/api/clientes/**` | F1 | `VITE_CLIENTES_API_URL=<orq>/api` |
| R5 monitor | directo a :8085 | `/api/monitor/**` | F1 | `VITE_MONITOR_API_URL=<orq>/api` |
| Sondeo directo de respaldo (`monitorClient.js`) | a cada servicio | **No** pasa por el orquestador; o se reemplaza por `GET /api/orquestador/servicios` | F2 | Decidir con el equipo del front |
| Estado del flujo de un lead | no existe | `GET /api/orquestador/flujos?leadId=` (opcional, para mostrar "calificando…", "generando resumen…") | F2 | Nuevo uso opcional |

Mientras tanto, a futuro, todas las `VITE_*_API_URL` podrían colapsar en `VITE_API_URL` (que ya existe y apunta a `:8080/api`).

### 4.2 leads — `LeadsMicroservice`

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| Entrante: `POST/GET /leads`, `GET /leads/{id}`, `PATCH /leads/{id}/state` | Llegan vía `/api/leads/**` | F1 | Ninguno |
| R6 `POST /clientes` | Vía `/api/clientes` | F1 | `CLIENTES_URL=<orq>/api` |
| K1 publica `preformulario-completado` | Sigue publicando leads; el orquestador **escucha** y avanza el flujo | F1 | Ninguno |
| K2 consume `lead-calificado` | Igual | — | Ninguno |
| K3 publica `resumen-solicitado` | **Pasa al orquestador**: cuando ve `lead-calificado`, él publica `resumen-solicitado` con el mismo contrato | F2 | Quitar `publish_resumen_solicitado` del consumer (o ponerlo detrás de un flag) |
| K4 consume `resumen-generado` | Igual | — | Ninguno |
| Salud | El orquestador necesita un `/health` | F2 | Agregar `GET /health` (resuelve I8) |

En la fase 2, para que el orquestador pueda armar `resumen-solicitado` sin consultar a leads, el flujo guarda los datos comerciales del paso (`anticipo`, `permuta`, `ubicacion`, etc.) del `preformulario-completado`. Se guardan solo mientras el flujo está activo (§6.3). La otra opción es que el orquestador haga `GET /leads/{id}` en ese momento, que es más simple y no duplica datos. **Recomendado: `GET /leads/{id}`.**

### 4.3 calificacion — `CalificacionMicroservice`

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| Entrante: `GET /calificaciones/{leadId}`, `POST .../correcciones` | Vía `/api/calificaciones/**` | F1 | Ninguno |
| R7 `GET /vehiculos/{id}` | Vía `/api/vehiculos` | F1 | `VEHICULOS_SERVICE_URL=<orq>/api` |
| R8 `GET /clientes/{id}/situacion-crediticia` | Vía `/api/clientes` | F1 | `CLIENTES_SERVICE_URL=<orq>/api` |
| K1 consume / K2 publica | Igual; el orquestador escucha K2 | F1 | Ninguno |
| K5 DLT `preformulario-completado-dlt` | El orquestador **consume la DLT** y marca el flujo como `FALLIDO` en el paso de calificación | F1 | Ninguno |
| Sobre en `lead-calificado` (I1) | El orquestador correlaciona por `leadId` porque no hay `eventId`/`correlationId` | F1 | Recomendado: publicar con el sobre común (F2) |

Una corrección manual (`POST .../correcciones`) publica otro `lead-calificado` con `origen=MANUAL`. El orquestador lo registra como paso del flujo `CORRECCION_CALIFICACION` y **no** relanza el resumen (a confirmar con el equipo).

### 4.4 vehiculos — `VehiculosMicroservice`

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| Entrante de lectura (front, calificacion, ia-resumen) | Vía `/api/vehiculos/**` | F1 | Ninguno |
| Entrante de escritura (`POST`, `PUT`, `PUT /foto`) desde los scripts `cargar_*.py` | Vía `/api/vehiculos/**`, pass-through de `X-Api-Key` | F1 | Apuntar los scripts al orquestador (opcional) |
| Kafka | No usa | — | — |

Es el servicio que menos cambia: solo recibe.

### 4.5 ia-resumen — `IAllmFormulario`

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| R9 `GET /clientes/{id}` | Vía `/api/clientes`; en F2 el orquestador inyecta la `X-Api-Key` que hoy falta (I10) | F1 | `CLIENTES_URL=<orq>/api` |
| R10 `GET /vehiculos/{id}` | Vía `/api/vehiculos` | F1 | `VEHICULOS_URL=<orq>/api` |
| R11 Ollama | **No**: es un proveedor externo, no un microservicio | — | — |
| K3 consume `resumen-solicitado` | Igual. En F2 lo publica el orquestador **con el mismo contrato**, así que ia-resumen no se entera del cambio | — | Ninguno |
| K4 publica `resumen-generado` | Igual; el orquestador escucha y cierra el flujo | F1 | Ninguno |
| `POST /resumen`, `/resumen-ia` | Vía `/api/resumen*` si alguien los usa | F1 | Ninguno |
| Dockerfile | No tiene; para el compose del ecosistema hace falta | F2 | Agregar Dockerfile (responsabilidad del repo) |

### 4.6 monitor — `MonitorMicroservice`

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| Entrante `/monitor/**` (front) | Vía `/api/monitor/**` | F1 | Ninguno |
| R12 health checks a cada servicio | **No** pasan por el orquestador: el monitor tiene que ver a cada servicio directo para diagnosticar bien | — | — |
| Objetivos monitoreados | Agregar **orquestador** (`/health`) e **ia-resumen** (`/openapi.json`) | F2 | Dos entradas más en `crm.monitor.servicios` |

**Reparto con el orquestador:** el monitor hace chequeos **activos** (sondea cada 15 s aunque no haya tráfico). El orquestador hace observación **pasiva** (mide las llamadas reales que pasan por él y el tiempo entre pasos de cada flujo). Se complementan. El monitor contesta "¿está vivo?". El orquestador contesta "¿está respondiendo bien a los pedidos reales y en qué paso se trabó cada flujo?".

### 4.7 clientes — `ClientMicroservice`

Ya existe y está probado detrás del proxy (alta con clave, ficha con `ETag`, `PATCH` con `If-Match`, situación crediticia e historial). Detalle en [01 §3.7](01-relevamiento-ecosistema.md).

| Interacción | Con orquestador | Fase | Cambio en el repo |
|---|---|---|---|
| Entrante R4, R6, R8, R9 | Vía `/api/clientes/**`. El proxy ya pasa `ETag`, `If-Match` y `X-Usuario` | F1 | Ninguno |
| `GET /clientes/{id}` sin clave | Confirmado: no exige `X-Api-Key`, así que ia-resumen (I10) funciona sin cambios | F1 | Ninguno |
| Salud | `/actuator/health` (el monitor ya lo usa) | — | Ninguno |
| R13 consulta al BCRA | Hoy es una fuente **simulada** dentro de clientes. Si pasa a BCRA real, es un proveedor externo y **no** pasa por el orquestador | — | — |
| K2 consume `lead-calificado` | Igual; el orquestador también lo escucha. En el objetivo final pasa a ser el comando `RegistrarCalificacion` en `crm.comandos` | F1 / final | Pasar a 3 tópicos |
| Publica `cliente-actualizado` y `cliente-anonimizado` | Nadie los consume hoy. leads los necesita para mantener sus copias; en el objetivo final pasan a `crm.eventos` y el orquestador decide a quién avisar | final | Pasar a 3 tópicos |
| DLT propia `lead-calificado.clientes.DLT` | Pasa a `crm.dlt` | final | Pasar a 3 tópicos |
| Formato de `fecha` de `lead-calificado` | Era incompatible con calificacion (array vs ISO-8601) y todo iba a la DLT. **Corregido** en calificacion (rama `fix/fecha-lead-calificado-iso`, PR pendiente) | F1 | Ver [05](05-cambios-en-otros-repos.md) |

Las escrituras del personal (`PATCH`, no contactar, anonimización) hoy no piden clave porque no hay autenticación: cuando entre **auth** (§4.8) pasan a ser rutas protegidas y el orquestador inyecta `X-Usuario`.

### 4.8 auth* (cuando se sume)

Es el servicio que **más cambia al orquestador**, porque lo convierte en el punto de control de acceso.

| Interacción | Con orquestador | Fase |
|---|---|---|
| Front `POST /auth/login` | Vía `/api/auth/**` (ruta pública) | F1 |
| Validación del token | El orquestador valida el JWT **localmente** (firma con clave pública/JWKS de auth, o secreto compartido) en cada request de rutas protegidas. **No** llama a auth en cada request: así, si auth se cae, los telemarketers ya logueados siguen trabajando | F1 |
| Identidad hacia los servicios | El orquestador reenvía `X-Usuario` (y `X-Usuario-Rol`, si hay roles) sacados del token. **`X-Usuario` es el header que clientes ya lee** para registrar quién operó. El orquestador **descarta cualquier `X-Usuario` que venga de afuera**, para que nadie se haga pasar por otro telemarketer. Los servicios confían en ese header porque solo el orquestador los alcanza | F2 |

Clasificación de rutas:

| Pública (prospecto, sin login) | Protegida (telemarketer) |
|---|---|
| `GET /api/vehiculos?disponible=true`, `GET /api/vehiculos/{id}` | `GET /api/leads`, `GET /api/leads/{id}`, `PATCH /api/leads/{id}/state` |
| `POST /api/leads` | `GET /api/calificaciones/**`, `POST /api/calificaciones/{id}/correcciones` (el `usuario` sale del token) |
| `POST /api/auth/login` | `GET /api/clientes/**`, `/api/monitor/**`, `/api/orquestador/**` |

El tráfico **entre servicios** no usa el token del telemarketer: se autentica con la clave de cada servicio (§3.2).

### 4.9 scraper de multas — `InfraccionScrapper`

**Decisión (2026-11-14):** las multas son de los **autos en stock** de la agencia. **No forman parte del flujo del lead.** Son un flujo propio entre el orquestador, el scraper y vehiculos. En el ecosistema el servicio se llama **multas** (el repo se llama `InfraccionScrapper`).

**Estado:** hoy es una librería y CLI de Node que consulta la ANSV por patente (ver [01 §3.9](01-relevamiento-ecosistema.md)). Para entrar al flujo tiene que convertirse en un servicio que escuche comandos por Kafka (lista en [05](05-cambios-en-otros-repos.md)).

| Interacción | Con orquestador | Fase |
|---|---|---|
| Pedido de deudas por patente | **Asincrónico por Kafka** (decidido 2026-11-14): el orquestador publica el comando `ConsultarMultas` en `crm.comandos` con `destino: multas`, y el scraper responde con el evento `MultasConsultadas` (o `ConsultaMultasFallida`) en `crm.eventos`. El scraping es lento y falla seguido; por REST bloquearía al que llama. Ver §5.2 | F3 |
| Guardar el resultado en el vehículo | El orquestador manda el resultado a vehiculos: `PUT /vehiculos/{id}/multas` o comando `vehiculos.comandos`. **vehiculos decide** qué hace con eso (guardarlo, marcar la unidad, etc.) | F3 |
| Consulta puntual | `GET /api/multas/{patente}` vía proxy, si hace falta | F1 |
| Scraping a sitios externos | **No** pasa por el orquestador | — |

Flujo propio `CONSULTA_MULTAS_VEHICULO`, con clave de negocio `vehiculoId`:

```
SOLICITADA ──► CONSULTANDO_MULTAS ──► (multas.eventos) ──► ACTUALIZANDO_VEHICULO ──► COMPLETADO
                     │ timeout / error del scraper                │ error de vehiculos
                     ▼                                            ▼
               REINTENTO (backoff, máx. N) ──► FALLIDO         FALLIDO
```

- **Disparadores:**
  - El alta de una unidad: el `POST /api/vehiculos` pasa por el orquestador y, si responde 201, se abre el flujo.
  - Un barrido periódico (ej. diario) de las unidades disponibles. El orquestador agenda el flujo; **qué unidades corresponde consultar lo responde vehiculos**, por ejemplo con `GET /vehiculos?disponible=true`.
  - Un pedido manual: `POST /api/orquestador/multas/{vehiculoId}`.
- **El orquestador** decide la frecuencia, los reintentos y el backoff, y respeta un límite de concurrencia para no saturar al scraper ni a los sitios que scrapea. Distingue los errores del scraper: `ErrorSitioNoDisponible` se reintenta con backoff; `ErrorEstructuraNoReconocida` (el sitio cambió) **no** se reintenta y se alerta; una patente sin multas es un resultado válido, no un error.
- **Datos personales:** el resultado del scraper (actas, importes, lugares) es dato personal. El orquestador **no lo guarda**: solo registra estado, tiempos y la cantidad de actas, igual que el log del scraper. El detalle va a vehiculos.
- **El scraper** decide cómo scrapear y cómo interpretar las deudas.
- **vehiculos** decide qué significa tener multas.
- **Requiere que vehiculos agregue el campo `patente`.** Es una migración de Flyway; la hace el responsable de vehiculos (Naim), según la regla de su repo.

### 4.10 Resumen: qué queda fuera del orquestador

- Llamadas a **proveedores externos**: Ollama (R11), BCRA (R13), sitios de multas (R16), Supabase Storage (vehiculos).
- **Health checks del monitor** (R12).
- **Mensajes de Kafka entre servicios**: siguen yendo por Kafka. El orquestador los escucha, no los intercepta.
- Acceso de cada servicio a **su propia base**.

## 5. Kafka en el orquestador

### 5.1 Tópicos

| Tópico | Rol del orquestador | Fase | Para qué |
|---|---|---|---|
| `preformulario-completado` | Consume | F1 | Flujo → `CALIFICACION_PENDIENTE` |
| `lead-calificado` | Consume | F1 | Flujo → `CALIFICADO` (o paso de corrección) |
| `resumen-solicitado` | Consume (F1) → **Produce** (F2) | F1/F2 | Flujo → `RESUMEN_PENDIENTE` |
| `resumen-generado` | Consume | F1 | Flujo → `COMPLETADO` |
| `preformulario-completado-dlt` | Consume | F1 | Flujo → `FALLIDO` |
| ~~`orquestador-dlt`~~ | — | — | **Descartado:** el plan free de Aiven permite 5 tópicos y ya están en uso. Ver §5.2. |

- **Grupo consumidor:** `orquestador`, propio, para no robarle mensajes a los demás grupos. Cada grupo recibe su copia completa.
- **Clave:** `leadId` al publicar, igual que el resto (orden por lead).
- **`auto_offset_reset="earliest"`** y commit del offset **después** de persistir el cambio de estado, para que una caída no pierda pasos.
- **Idempotencia:** tabla `evento_procesado`. La clave es `eventId` cuando viene, y si no (K2 llega sin sobre, I1) se usa `topic:partition:offset`.
- **Formatos:** aceptar mensajes con y sin sobre (`payload` o raíz), como ya hacen leads y calificacion.
- **Config:** misma que leads (`SASL_SSL` + `ssl_context` desde `ca.pem`, `KAFKA_ENABLED`, creación de tópicos al arrancar). Si Kafka no está, la API HTTP levanta igual (como ia-resumen) y el estado lo informa `/health`.

### 5.2 Límite de tópicos de Aiven: esquema de 3 tópicos

**Decisión (2026-11-14):** el plan free de Aiven permite **5 tópicos** y ya están todos en uso. El diseño de "un tópico de comandos y otro de eventos por servicio" (§2) no entra: serían unos 16. Se reemplaza por **3 tópicos separados por tipo de mensaje**:

| Tópico | Publica | Escucha | Contenido |
|---|---|---|---|
| `crm.comandos` | Solo el orquestador | Todos los servicios (cada uno con su grupo) | Órdenes. El sobre lleva `destino`; cada servicio ignora lo que no es suyo. |
| `crm.eventos` | Todos los servicios | Solo el orquestador | Hechos (`PreformularioCompletado`, `LeadCalificado`, `ResumenGenerado`...). |
| `crm.dlt` | Cualquiera | Monitoreo / orquestador | Mensajes que fallaron, de cualquier servicio. |

Mapeo de lo que hay hoy (incluye los dos servicios nuevos):

| Hoy | Pasa a |
|---|---|
| `preformulario-completado` (leads) | evento `PreformularioCompletado` en `crm.eventos` |
| `lead-calificado` (calificacion) | evento `LeadCalificado` en `crm.eventos`; clientes y leads dejan de escucharlo y reciben comandos (`RegistrarCalificacion`, etc.) |
| `resumen-solicitado` / `resumen-generado` | comando `GenerarResumen` en `crm.comandos` / evento `ResumenGenerado` en `crm.eventos` |
| `cliente-actualizado`, `cliente-anonimizado` (clientes) | eventos `ClienteActualizado` y `ClienteAnonimizado` en `crm.eventos` |
| comando de multas (nuevo) | `ConsultarMultas` en `crm.comandos` (`destino: multas`); respuesta `MultasConsultadas` en `crm.eventos` |
| `preformulario-completado-dlt`, `lead-calificado.clientes.DLT` | `crm.dlt` |

La clave sigue siendo el `leadId` (o el `clienteId` / `vehiculoId` según el flujo), así que se mantiene el orden por entidad. El costo es que cada servicio lee órdenes ajenas y las descarta, algo insignificante con ~150 leads por día. Se prototipa en local (Kafka del compose, sin límite) y se aplica en Aiven en un cambio coordinado, porque los 5 tópicos viejos y los 3 nuevos no pueden convivir. Los cambios por repo están en [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md).

### 5.3 Contrato de eventos que publica el orquestador

Sobre común del documento v1.1, **completo** (con `version` y `occurredAt`), más `timestamp` por compatibilidad con los consumidores Python actuales:

```json
{
  "eventId": "uuid",
  "eventType": "ResumenSolicitado",
  "version": 1,
  "occurredAt": "2026-11-14T12:00:00Z",
  "timestamp": "2026-11-14T12:00:00Z",
  "correlationId": "uuid",
  "payload": { "leadId": "uuid", "...": "mismo payload que publica hoy leads" }
}
```

## 6. Base de datos (Supabase PostgreSQL)

### 6.1 Conexión

Mismo esquema que vehiculos, que es el más prolijo:

- Proyecto Supabase compartido, **schema `orquestador`** y **rol propio `orquestador_app`** con permisos solo sobre ese schema.
- **Session pooler, puerto 5432**, `sslmode=require`.
- Pool chico, como leads: `pool_size=3`, `max_overflow=0`, `pool_pre_ping=True`, `pool_recycle=180`. El pooler de sesión permite 15 clientes en total **para todo el equipo**.
- Migraciones versionadas con **Alembic** (el equivalente de Flyway en Python), no `create_all`.
- Driver async (`postgresql+psycopg` con SQLAlchemy async) para no bloquear el loop del proxy.

### 6.2 Modelo propuesto

**Sobre los nombres.** Lo que en la teoría de microservicios se llama **saga** (§2) acá se llama **flujo**, porque al equipo le resulta más claro (decisión del 2026-11-14):

- Un **flujo** es una ejecución de un proceso de negocio de varios pasos. Por ejemplo, "el registro del lead X".
- Un **paso** es cada interacción con un servicio dentro de ese flujo. Por ejemplo, "calificacion respondió en 1,2 s".

La migración `0002` renombró las tablas, que originalmente se llamaban `saga` y `saga_paso`.

```
flujo
├─ id                uuid PK
├─ tipo              text      REGISTRO_LEAD | CORRECCION_CALIFICACION | ...
├─ clave_negocio     text      leadId (índice)
├─ correlation_id    text      (índice)
├─ estado            text      ver §6.4
├─ paso_actual       text
├─ intentos          int
├─ ultimo_error      text null
├─ vence_en          timestamptz null   deadline del paso actual
├─ iniciado_en       timestamptz
├─ actualizado_en    timestamptz
├─ finalizado_en     timestamptz null
└─ version           int       bloqueo optimista

flujo_paso
├─ id                bigserial PK
├─ flujo_id          uuid FK → flujo
├─ paso              text      ej. CALIFICAR_LEAD
├─ servicio          text      leads | calificacion | ia-resumen | ...
├─ canal             text      REST | KAFKA
├─ estado            text      PENDIENTE | OK | ERROR | TIMEOUT
├─ intento           int
├─ iniciado_en       timestamptz
├─ respondido_en     timestamptz null
├─ latencia_ms       int null  respondido_en - iniciado_en
├─ codigo_http       int null
├─ referencia        text null eventId / topic:partition:offset
└─ detalle_error     text null

llamada_servicio          (auditoría del proxy; con retención, ej. 7 días)
├─ id                bigserial PK
├─ correlation_id    text
├─ origen            text      front | leads | calificacion | ... | desconocido
├─ destino           text
├─ metodo            text
├─ ruta              text      sin query string
├─ codigo_http       int null
├─ latencia_ms       int
├─ error             text null CONEXION | TIMEOUT | ...
└─ ocurrida_en       timestamptz (índice)

evento_procesado          (idempotencia de consumo)
├─ clave             text PK   eventId o topic:partition:offset
├─ topic             text
└─ procesado_en      timestamptz

outbox                    (fase 2: publicación confiable de comandos)
├─ id                uuid PK
├─ topic             text
├─ clave             text
├─ mensaje           jsonb
├─ creado_en         timestamptz
└─ publicado_en      timestamptz null
```

### 6.3 Qué se guarda y qué no

- **Sí:** estados, pasos, tiempos, códigos, IDs (UUID de lead/cliente/vehículo, que no son datos personales), mensajes de error técnicos.
- **No:** cuerpos de request/response, nombre, email, teléfono, CUIL, montos. En el happy path la base solo recibe unos pocos `INSERT`/`UPDATE` por lead.
- `outbox.mensaje` (fase 2) es la única excepción: contiene el payload del comando a publicar (datos comerciales, sin datos personales, igual que hoy viajan por Kafka). Se purga al publicarse.

### 6.4 Máquina de estados: flujo `REGISTRO_LEAD`

**Implementado en la fase 1b** (`app/flujos/registro_lead.py`):

```
           K1 preformulario-completado (leads)
                       │
                       ▼
            CALIFICACION_PENDIENTE ──── K5 DLT ──► FALLIDO
                       │ K2 lead-calificado
                       ▼
                   CALIFICADO
                       │ K3 resumen-solicitado (leads)
                       ▼
               RESUMEN_PENDIENTE
                       │ K4 resumen-generado (ia-resumen)
                       ▼
                   COMPLETADO

   Desde CALIFICACION_PENDIENTE, CALIFICADO o RESUMEN_PENDIENTE, si se vence
   el plazo → EXPIRADO. Si después llega el evento que faltaba, el flujo sigue.
```

- **El flujo nace con `preformulario-completado`, no con el 202 del proxy.** El proxy reenvía la respuesta en streaming sin leerla, así que no conoce el `leadId`. Además, el evento ya trae el `correlationId` original del front. Los `POST /api/leads` que fallan antes de crear el lead se ven en `/api/orquestador/servicios`.
- **Pasos con latencia:**
  - `CALIFICAR_LEAD` mide de K1 a K2: cuánto tardó calificacion.
  - `GENERAR_RESUMEN` mide de K3 a K4: cuánto tardó la IA.
  - Se usa el timestamp de cada mensaje en Kafka, así todos se miden con el mismo reloj.
- **Vencimientos:** cada `FLUJO_INTERVALO_VENCIMIENTO_SEGUNDOS`, una tarea marca `EXPIRADO` lo que venció. El paso pendiente queda en `TIMEOUT` y el flujo guarda en `ultimo_error` qué servicio no respondió. Los plazos son `FLUJO_TIMEOUT_CALIFICACION_SEGUNDOS` y `FLUJO_TIMEOUT_RESUMEN_SEGUNDOS`. Para `CALIFICADO` se usa el plazo del resumen, que es lo que espera de leads.
- **Robustez:**
  - Los duplicados se descartan con `evento_procesado`.
  - Los eventos desordenados nunca hacen retroceder el estado.
  - Dos eventos simultáneos del mismo lead no duplican el flujo, gracias a la restricción única `(tipo, clave_negocio)` y al bloqueo `FOR UPDATE`.
  - Una corrección manual (`lead-calificado` con `origen=MANUAL`) se registra como paso `CORRECCION_CALIFICACION` sin cambiar el estado.

### 6.5 Recuperación

| Situación | Qué hace el orquestador |
|---|---|
| **Se cae el orquestador** | Al volver, el consumidor Kafka retoma desde el último offset confirmado y avanza los flujos con los eventos acumulados. Las requests HTTP en vuelo se pierden; el que llamó recibe error y reintenta, como hoy. |
| **Se cae calificacion** | Los K1 esperan en Kafka. Los flujos quedan en `CALIFICACION_PENDIENTE` y, al vencer, pasan a `EXPIRADO` con alerta. Cuando calificacion vuelve y publica K2, el flujo **sale de `EXPIRADO`** y sigue. |
| **Se cae ia-resumen** | Igual, en `RESUMEN_PENDIENTE`. En F2 el orquestador puede **re-publicar** `resumen-solicitado` (`POST /api/orquestador/flujos/{id}/reintentar`); como leads sobrescribe el resumen, repetir es seguro. |
| **Se cae un servicio en una llamada REST** | El proxy registra `503`/`504` en `llamada_servicio`; la tasa de errores por servicio se ve en `GET /api/orquestador/servicios`. |
| **Mensaje duplicado** | `evento_procesado` lo descarta (RNF-04). |

## 7. Implementación

### 7.1 Stack

| Pieza | Elección | Referencia |
|---|---|---|
| Framework | FastAPI + uvicorn | leads, ia-resumen |
| Config | `pydantic-settings` (variables de entorno, `.env` en local) | mejora sobre `os.getenv` |
| HTTP saliente | `httpx.AsyncClient` | leads |
| Kafka | `aiokafka` con `ssl_context` | leads |
| DB | SQLAlchemy 2 async + psycopg 3 + Alembic | leads + Flyway de vehiculos |
| Tests | pytest + pytest-asyncio; `testcontainers` para Postgres/Kafka | vehiculos usa Testcontainers |

### 7.2 Estructura de carpetas propuesta

```
app/
├─ main.py                 lifespan: DB, producer, consumer, httpx client
├─ config.py               Settings (pydantic-settings)
├─ api/
│  ├─ proxy.py             /api/{servicio}/** → servicio destino
│  ├─ flujos.py            /api/orquestador/flujos
│  ├─ servicios.py         /api/orquestador/servicios (métricas)
│  └─ health.py            /health
├─ middleware/
│  └─ correlation_id.py
├─ kafka/
│  ├─ config.py            seguridad SASL_SSL, tópicos
│  ├─ consumer.py          despacho por tópico
│  └─ producer.py          sobre común + outbox
├─ flujos/
│  ├─ maquina.py           transiciones genéricas
│  └─ registro_lead.py     definición del flujo REGISTRO_LEAD
├─ db/
│  ├─ session.py
│  └─ models.py
└─ errores.py              ProblemDetail
alembic/
tests/
Dockerfile
docker-compose.yml
.env.example
ca.pem
```

### 7.3 Endpoints propios

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/health` | Estado propio + DB + Kafka (para el monitor y el healthcheck de Docker) |
| GET | `/api/orquestador/flujos?leadId=&estado=&tipo=` | Buscar flujos |
| GET | `/api/orquestador/flujos/{id}` | Flujo con todos sus pasos y latencias |
| POST | `/api/orquestador/flujos/{id}/reintentar` | Reintentar el paso actual (F2) |
| GET | `/api/orquestador/servicios` | Por servicio: llamadas, % de error, latencia p50/p95, último error (ventana configurable) |
| GET | `/docs`, `/openapi.json` | Swagger de FastAPI |

### 7.4 Variables de entorno

```dotenv
# --- app ---
PORT=8080
CORS_ALLOWED_ORIGINS=http://localhost:5173,http://localhost:3000

# --- servicios destino (URL base, sin el recurso) ---
LEADS_URL=http://localhost:8000
CLIENTES_URL=http://localhost:8082
VEHICULOS_URL=http://localhost:8083
CALIFICACION_URL=http://localhost:8084
MONITOR_URL=http://localhost:8085
# ia-resumen no fija puerto (en Render usa $PORT; en local uvicorn usa 8000 y choca con leads)
IA_RESUMEN_URL=http://localhost:8001
PRECALIFICACION_URL=
PROXY_TIMEOUT_SEGUNDOS=5

# --- base (Supabase, session pooler 5432) ---
DATABASE_URL=postgresql+psycopg://orquestador_app.<project-ref>:<clave>@<host-pooler>:5432/postgres?sslmode=require
DB_SCHEMA=orquestador

# --- kafka ---
KAFKA_ENABLED=true
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMER_GROUP=orquestador
KAFKA_CLIENT_ID=orquestador
KAFKA_SECURITY_PROTOCOL=
KAFKA_SASL_MECHANISM=
KAFKA_SASL_USERNAME=
KAFKA_SASL_PASSWORD=
KAFKA_SSL_CAFILE=ca.pem

# --- flujos ---
FLUJO_TIMEOUT_CALIFICACION_SEGUNDOS=120
FLUJO_TIMEOUT_RESUMEN_SEGUNDOS=300
AUDITORIA_RETENCION_DIAS=7
```

### 7.5 Docker

- **`Dockerfile`:** `python:3.12-slim`, usuario no root (como vehiculos), `pip install --no-cache-dir`, copia `ca.pem`, `HEALTHCHECK` contra `/health`, `CMD uvicorn app.main:app --host 0.0.0.0 --port 8080`. Las migraciones (`alembic upgrade head`) corren antes de uvicorn en un entrypoint, o como servicio aparte en el compose.
- **`docker-compose.yml` de desarrollo:**
  - `orquestador` (build local, `env_file: .env`, variables obligatorias con `${VAR:?mensaje}` como vehiculos).
  - `kafka` en **modo KRaft** (imagen `apache/kafka`, sin Zookeeper, como pide AD-03), para no depender de Aiven en local.
  - `kafka-ui` opcional (puerto 8090, como prevé el documento).
  - `postgres` local opcional, en un *profile* `local-db`, para no gastar conexiones del pooler compartido mientras se desarrolla.
- Más adelante, un compose del **ecosistema completo** (todos los servicios + orquestador) cumpliría RNF-08 ("todo se levanta con un solo comando"). Necesita que ia-resumen tenga Dockerfile.

## 8. Plan por fases

| Fase | Objetivo | Cambios en otros repos |
|---|---|---|
| **0. Esqueleto** | FastAPI + `/health` + Dockerfile + compose + config + Alembic con las tablas | Ninguno |
| **1a. Proxy** ✅ | `/api/{servicio}/**` con correlation id, auditoría en `llamada_servicio` y `/api/orquestador/servicios`. Despliegue en Render: [03-despliegue-render.md](03-despliegue-render.md) | Solo URLs en `.env` (front, leads, calificacion, ia-resumen) |
| **1b. Seguimiento de flujos** ✅ | Consumir K1–K5, máquina `REGISTRO_LEAD`, timeouts y `/api/orquestador/flujos` | Ninguno |
| **2. Orquestación real** | El orquestador publica `resumen-solicitado` (outbox), reintentos manuales, inyección de `X-Api-Key`, `X-Origen-Servicio` | leads deja de publicar K3; leads agrega `/health`; monitor suma orquestador e ia-resumen; calificacion publica con sobre (recomendado) |
| **3. Nuevos servicios** | clientes y auth (validación de JWT en el orquestador) al ruteo; flujo `CONSULTA_MULTAS_VEHICULO` con el scraper y vehiculos | Los de esos repos; el front agrega login |
| **4. Orquestación completa** | Tópicos de comandos y eventos por servicio; ningún servicio escucha a otro | Todos: cambiar qué tópico escucha y en cuál publica cada uno |

## 9. Decisiones abiertas para el equipo

1. ~~¿El orquestador reemplaza al API Gateway del documento (AD-10)?~~ **Resuelto (2026-11-14): sí.** El orquestador es el API Gateway del proyecto: puerto 8080, prefijo `/api`. Falta registrarlo como AD-16 en el documento de arquitectura.
2. **Fase 2 de `resumen-solicitado`:** ¿se acepta que leads deje de publicarlo? Es el único cambio de código obligatorio en otro repo para que haya orquestación real.
3. **Autenticación servicio → orquestador:** ¿pass-through de `X-Api-Key` (más simple) o claves por servicio con inyección en el orquestador (más seguro, centraliza secretos)?
4. **Correcciones manuales:** ¿una corrección debe volver a generar el resumen de la IA?
5. **Sobre de eventos:** ¿se normaliza en todos los servicios al formato del documento (`version`, `occurredAt`) o el orquestador se queda tolerando las variantes?
6. **Base:** confirmar con el responsable de Supabase (Naim) la creación del schema `orquestador` y del rol `orquestador_app`, y el presupuesto de conexiones del pooler.
7. **Kafka:** confirmar el mecanismo SASL de Aiven (`PLAIN` o `SCRAM-SHA-256`, I12) y cómo se hace el pase a los 3 tópicos (§5.2).
8. **Scraper de multas** (resuelto: stock):
   - ¿Con qué frecuencia se re-consultan las unidades?
   - ¿vehiculos expone `PUT /vehiculos/{id}/multas` o escucha un comando por Kafka?
   - ¿Las multas afectan algo visible (precio, disponibilidad)? Eso lo decide vehiculos.
9. **Auth:** ¿JWT firmado con clave asimétrica (el orquestador solo necesita la pública) o secreto compartido? ¿Hay roles además del telemarketer?
10. **BCRA:** ¿desaparece el servicio precalificacion y la consulta la hace clientes directo?
11. **Front:** ¿quieren mostrar el estado del flujo (`/api/orquestador/flujos?leadId=`) en la cola o en la confirmación del preformulario?
