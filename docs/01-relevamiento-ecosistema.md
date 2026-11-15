# Relevamiento del ecosistema

Fotografía del estado actual de los repos que viven junto a este en `DdA2_Repos/`, leída del código (no solo de los README). Sirve como punto de partida para decidir qué tráfico absorbe el orquestador.

## 1. Repos y rama base

Todos los repos hermanos están en `main`, limpios. Es la rama desde la que partimos.

| Repo | Rol | Rama actual | Último commit | Remoto |
|---|---|---|---|---|
| `DDA2` | Frontend (React + Vite) | `main` | `b60e4c3` (2026-11-14) | github.com/manuherrera09/DDA2 |
| `LeadsMicroservice` | leads | `main` | `c0901d4` (2026-11-14) | github.com/naimgaunaa/LeadsMicroservice |
| `CalificacionMicroservice` | calificacion | `main` | `03bbd76` (2026-10-12) | github.com/naimgaunaa/CalificacionMicroservice |
| `VehiculosMicroservice` | vehiculos | `main` | `8035cfb` (2026-11-14) | github.com/naimgaunaa/VehiculosMicroservice |
| `IAllmFormulario` | ia-resumen | `main` | `9325169` (2026-11-08) | github.com/JoaquinUade/IAllmFormulario |
| `MonitorMicroservice` | monitor | `main` | `5ec15f3` (2026-11-08) | github.com/manuherrera09/MonitorMicroservice |
| `DdA2_Microservices_Orchestrator_G8` | **este orquestador** | `dev` | `1393416` (Initial commit) | github.com/FrancoF04/DdA2_Microservices_Orchestrator_G8 |

### Los 3 microservicios que faltan

Confirmado por el equipo (2026-11-14): faltan **3** servicios, así que el ecosistema queda en **8 microservicios + orquestador + front**.

| Servicio | Responsable | Puerto esperado | Qué hace / evidencia |
|---|---|---|---|
| **clientes** | Juan | 8082 | Datos personales, CUIL, consentimiento, situación crediticia e historial. **Ya existe** (repo `ClientMicroservice`, Spring Boot): ver §3.7. Lo llaman leads (`POST /clientes`), calificacion (`GET /clientes/{id}/situacion-crediticia`), ia-resumen y el front. |
| **auth** | en el equipo | 8086 | Autenticación del **telemarketer** (login). **Ya existe** (repo `AuthMicroservice`, FastAPI): ver §3.8 y [06-autenticacion.md](06-autenticacion.md). |
| **scraper de multas** | en desarrollo | 8087 (propuesto) | Obtiene deudas por multas asociadas a la **patente** de los **autos en stock**. **Existe como librería y CLI** (repo `InfraccionScrapper`, Node): ver §3.9. El servicio y el campo `patente` en vehiculos todavía faltan. |

- El servicio **precalificacion** (SOAP al BCRA) del documento v1.1 **no está en la lista**. clientes lo reemplaza con una fuente **simulada** (modo `simulado`); el adaptador al BCRA (`bcra`) queda para una segunda parte y hoy hace fallar el arranque si se activa. **[A confirmar si se hace]**
- De estos 3 servicios, **clientes** (§3.7) y **auth** (§3.8) ya son reales. **multas** sigue siendo la parte inferida (§3.9): el scraper existe como librería, pero todavía no como servicio.

## 2. Arquitectura actual en una mirada

```
                 ┌──────────── Front (DDA2) ─────────────┐
                 │ llama DIRECTO a cada servicio (no hay │
                 │ gateway desplegado)                   │
                 └──┬──────┬──────────┬──────────┬───────┘
                    │      │          │          │
                 vehiculos leads  calificacion clientes*   monitor   auth*   scraper-multas*
                  :8083   :8000     :8084       :8082      :8085    (nadie los llama todavía)
                    ▲      │ REST      │ REST      ▲  │
                    │      └──────────►┼──────────►│  └──► BCRA (¿directo?)
                    │ REST             │           │
                    ├──────────────────┘           │
                    │                              │
                 ia-resumen ───────── REST ────────┘
                    │
                    └──► Ollama Cloud (LLM)

  Kafka (Aiven, SASL_SSL):
   leads ──preformulario-completado──► calificacion ──lead-calificado──► leads, clientes*
   leads ──resumen-solicitado──► ia-resumen ──resumen-generado──► leads

  * = servicio que todavía no está en la carpeta
```

Hoy la integración es **coreografía pura**: nadie coordina el flujo; cada servicio reacciona al evento del anterior. Por ejemplo, es `leads` el que decide pedir el resumen a la IA cuando recibe `lead-calificado`.

## 3. Detalle por repo

### 3.1 Frontend — `DDA2`

- **Stack:** React + Vite, axios, react-router-dom.
- **Variables:** `VITE_API_URL` (gateway, hoy sin uso real), `VITE_VEHICULOS_API_URL`, `VITE_LEADS_API_URL`, `VITE_CALIFICACION_API_URL`, `VITE_CLIENTES_API_URL`, `VITE_MONITOR_API_URL`.
- **No pasa por ningún gateway:** `src/api/leadsClient.js` lo dice explícito ("No hay API Gateway en este despliegue"). Cada cliente axios apunta al puerto del servicio.

| Llama a | Método y ruta | Archivo | Uso |
|---|---|---|---|
| vehiculos | `GET /vehiculos` | `src/api/vehiclesClient.js` | Selector del preformulario (filtra en memoria) |
| vehiculos | `GET /vehiculos/{id}` | `vehiclesClient.js` | Datos del vehículo en la cola |
| vehiculos | `GET /vehiculos/{id}/precio`, `GET /vehiculos/{id}/historial-precios` | `vehiclesClient.js` | Definidos, sin uso en pantallas hoy |
| leads | `POST /leads` | `src/api/leadsClient.js` | Enviar preformulario (202 + `lead_id`) |
| leads | `GET /leads?estado=&nivel=` | `leadsClient.js` | Cola del telemarketer |
| leads | `GET /leads/{id}` | `leadsClient.js` | Ficha del lead |
| leads | `PATCH /leads/{id}/state?nuevo_estado=` | `leadsClient.js` | Tomar / derivar / descartar |
| calificacion | `GET /calificaciones/{leadId}` | `leadsClient.js` | Motivos de la calificación |
| calificacion | `POST /calificaciones/{leadId}/correcciones` | `leadsClient.js` | Corrección manual (`{ nuevoNivel, motivo }`) |
| clientes* | `GET /clientes/{id}` | `leadsClient.js` | Nombre/teléfono en la ficha y la cola |
| clientes* | `GET /clientes/{id}/historial` | `leadsClient.js` | Historial de calificaciones del cliente |
| monitor | `GET /monitor/estado?forzar=`, `GET /monitor/historial`, `POST /monitor/verificar`, `POST /monitor/despertar` | `src/api/monitorClient.js` | Página de monitor |
| todos | `GET /actuator/health`, `/health`, `/openapi.json`, `/v3/api-docs` | `monitorClient.js` | Sondeo directo de respaldo si el monitor no responde |

### 3.2 leads — `LeadsMicroservice`

- **Stack:** Python 3.12, FastAPI, SQLAlchemy + psycopg, aiokafka, httpx. **Es la referencia más cercana para el orquestador.**
- **Puerto:** 8000. **DB:** PostgreSQL (Supabase vía pooler de sesión; pool de 3 sin overflow porque el pooler limita a 15 clientes). Tablas con `Base.metadata.create_all` (sin migraciones).
- **Docker:** `Dockerfile` (`python:3.12-slim` + uvicorn) y `docker-compose.yml` con Postgres 16, Kafka (cp-kafka 7.7.1 + Zookeeper) y el servicio.
- **Kafka:** `app/kafka/config.py` arma `ssl.SSLContext` desde `KAFKA_SSL_CAFILE` (aiokafka no acepta `ssl_cafile`); crea los tópicos al arrancar porque Aiven no los autocrea; flag `KAFKA_ENABLED`.
- **Máquina de estados del lead:** `PENDIENTE → CALIFICADO → EN_GESTION → DERIVADO | DESCARTADO` (y `CALIFICADO → DESCARTADO`).

**Expone**

| Método | Ruta | Quién lo llama | Notas |
|---|---|---|---|
| POST | `/leads` | Front | 202 con `{ lead_id }`. Body camelCase (`cliente`, `vehiculoId`, `anticipo`, `permuta`, `ubicacion`, `solicitaFinanciacion`). Lee `X-Correlation-Id` o genera uno. |
| GET | `/leads?estado=&nivel=` | Front | Cola ordenada por nivel, puntaje y antigüedad. Respuesta en **snake_case**. |
| GET | `/leads/{id_lead}` | Front | Ficha con `cliente_id`, `resumen`, etc. |
| PATCH | `/leads/{id_lead}/state?nuevo_estado=` | Front | 400 si la transición es inválida. |

No expone `/health` (el monitor cae al fallback `/openapi.json`).

**Llama a**

| Destino | Método y ruta | Headers | Comportamiento |
|---|---|---|---|
| clientes* | `POST {CLIENTES_URL}/clientes` | `X-Api-Key: CLIENTES_CLAVE_LEADS`, `X-Correlation-Id` | Timeout 5 s, 2 intentos ante 5xx/timeout. 422 se propaga al front; 401 → 500; sin respuesta → 503; otro → 502. |

**Kafka**

| Dirección | Tópico | Evento | Clave | Cuándo |
|---|---|---|---|---|
| Publica | `preformulario-completado` | `PreformularioCompletado` (con sobre) | `leadId` | Tras crear el lead |
| Publica | `resumen-solicitado` | `ResumenSolicitado` (con sobre) | `leadId` | Al recibir `lead-calificado` |
| Consume | `lead-calificado` | `LeadCalificado` | — | Actualiza nivel/puntaje y pasa a `CALIFICADO` |
| Consume | `resumen-generado` | `ResumenGenerado` | — | Guarda `resumen` y `resumen_generado_en` |

Grupo consumidor `leads-service`. Sobre común: `{ eventId, eventType, correlationId, timestamp, payload }`.

### 3.3 calificacion — `CalificacionMicroservice`

- **Stack:** Java 21, Spring Boot 3.3.4, Spring Kafka, JPA, springdoc. Arquitectura hexagonal.
- **Puerto:** 8084. **DB:** PostgreSQL, schema `calificacion`, `ddl-auto: update`, Hikari con máx. 2 conexiones.
- **Docker:** `Dockerfile` multi-stage (maven → temurin 21 jre), copia `ca.pem` para el truststore PEM de Kafka.
- **Kafka:** SASL_SSL por variables `SPRING_KAFKA_PROPERTIES_*` (comentario: SCRAM-SHA-256). Reintentos 3 × 1 s y DLT.

**Expone**

| Método | Ruta | Quién lo llama |
|---|---|---|
| GET | `/calificaciones/{leadId}` | Front |
| GET | `/calificaciones/{leadId}/historial` | Nadie (el doc v1.1 dice que el historial pasó a clientes) |
| POST | `/calificaciones/{leadId}/correcciones` | Front. Body `{ nuevoNivel, motivo, puntos?, usuario? }`. Responde 201 y publica `LeadCalificado` con origen `MANUAL`. |

**Llama a**

| Destino | Método y ruta | Headers | Comportamiento |
|---|---|---|---|
| vehiculos | `GET {VEHICULOS_SERVICE_URL}/vehiculos/{id}` | — | Toma `precio`/`precioVenta`/`precioLista`. Timeout 3 s conexión / 5 s lectura. Si falla, sigue sin precio. **No usa `/precio`.** |
| clientes* | `GET {CLIENTES_SERVICE_URL}/clientes/{id}/situacion-crediticia` | `X-Api-Key: CLIENTES_CLAVE_CALIFICACION` | Toma `situacion`/`situacionCrediticia`/`situacionBcra`. Si falla, "sin datos". |

**Kafka**

| Dirección | Tópico | Evento | Clave |
|---|---|---|---|
| Consume | `preformulario-completado` | Acepta con o sin sobre; tolera `anticipo`/`montoAnticipo`, `provincia`/`ubicacion` | — |
| Publica | `lead-calificado` | `LeadCalificadoEvent` **sin sobre** (JSON plano) | `leadId` |
| Publica (error handler) | `preformulario-completado-dlt` | Mensajes que fallaron 3 veces | — |

Grupo consumidor `calificacion-group`.

### 3.4 vehiculos — `VehiculosMicroservice`

- **Stack:** Java 21, Spring Boot 3.5, JPA, **Flyway**, Supabase Storage, Testcontainers. Proyecto Maven multi-módulo (`vehiculos-service/`).
- **Puerto:** 8083. **DB:** Supabase compartido, **schema `vehiculos`**, rol propio `vehiculos_app.<project-ref>`, session pooler puerto 5432, `ddl-auto: validate`. **Es la referencia para la base del orquestador.**
- **Docker:** `Dockerfile` multi-stage con usuario no root y `curl` para el healthcheck; `infra/docker-compose.yml` que exige las variables obligatorias con `${VAR:?mensaje}`.
- **Transversales:** `CorrelationIdFilter` (lee/genera `X-Correlation-Id`, lo devuelve y lo pone en el MDC); escrituras protegidas con `X-Api-Key`; errores `application/problem+json` con `codigo` y `correlationId`.
- **Kafka:** ninguno.

**Expone**

| Método | Ruta | Clave | Quién lo llama |
|---|---|---|---|
| GET | `/vehiculos?disponible=` | No | Front |
| GET | `/vehiculos/{id}` | No | Front, calificacion, ia-resumen |
| GET | `/vehiculos/{id}/precio` | No | Nadie hoy (el doc dice que lo usa calificacion) |
| GET | `/vehiculos/{id}/historial-precios` | No | Nadie hoy |
| POST | `/vehiculos` | `X-Api-Key` | Carga por script |
| PUT | `/vehiculos/{id}` | `X-Api-Key` | Carga por script |
| PUT | `/vehiculos/{id}/foto` | `X-Api-Key` | Script `cargar_fotos.py` (multipart) |
| GET | `/actuator/health` | No | monitor, front |

**Llama a:** nadie (solo a Supabase Storage).

### 3.5 ia-resumen — `IAllmFormulario`

- **Stack:** Python, FastAPI, aiokafka, httpx, cliente `openai` contra **Ollama Cloud**. Desplegado en Render (`render.yaml`). **No tiene Dockerfile.**
- **Kafka:** misma config SASL_SSL que leads (ejemplo con mecanismo `PLAIN`). Si Kafka no levanta, la API sigue viva.

**Expone**

| Método | Ruta | Quién lo llama |
|---|---|---|
| POST | `/resumen` | Nadie en el flujo (resumen por plantilla, sin IA) |
| POST | `/resumen-ia` | Nadie en el flujo (resumen con IA, sincrónico) |

**Llama a**

| Destino | Método y ruta | Headers | Comportamiento |
|---|---|---|---|
| clientes* | `GET {CLIENTES_URL}/clientes/{id}` | **ninguno** | Timeout 5 s. Si falla, sigue sin nombre. |
| vehiculos | `GET {VEHICULOS_URL}/vehiculos/{id}` | — | Timeout 5 s. |
| Ollama | `POST {OLLAMA_BASE_URL}/chat/completions` | `OLLAMA_API_KEY` | Bloqueante, corre en thread. |

**Kafka**

| Dirección | Tópico | Evento | Clave |
|---|---|---|---|
| Consume | `resumen-solicitado` | `ResumenSolicitado` | — |
| Publica | `resumen-generado` | `ResumenGenerado` (con sobre): `{ leadId, resumen }` | `leadId` |

Grupo consumidor `resumen-ia-service`.

### 3.6 monitor — `MonitorMicroservice`

- **Stack:** Java 21, Spring Boot 3.3.4, Actuator, JPA. **DB por defecto H2 en memoria** (compatible con PostgreSQL por variables).
- **Puerto:** 8085. **Docker:** `Dockerfile` multi-stage. **Kafka:** ninguno.
- Sondea cada 15 s (paralelo), estados `UP` / `DEGRADADO` (≥ 1000 ms o 4xx) / `DOWN`. Ruta primaria + alternativa (fallback a OpenAPI si no hay health).

**Expone**

| Método | Ruta | Quién lo llama |
|---|---|---|
| GET | `/monitor/estado?forzar=` | Front |
| GET | `/monitor/historial?servicio=&limite=` | Front |
| POST | `/monitor/verificar?servicio=` | Front |
| POST | `/monitor/despertar?servicio=` | Front (cold start de Render) |
| GET | `/monitor/resumen` | Nadie hoy |
| GET | `/actuator/health` | — |

**Llama a:** `GET` de salud de vehiculos (`/actuator/health`), clientes (`/actuator/health`), leads (`/health` → `/openapi.json`) y calificacion (`/actuator/health`). **No monitorea ia-resumen** ni (obviamente) al orquestador.

### 3.7 clientes (repo `ClientMicroservice`)

Java 21 + Spring Boot 3.5, arquitectura hexagonal, PostgreSQL con Flyway (schema `clientes`), puerto 8082, salud en `/actuator/health`, Swagger en `/swagger-ui.html`. Errores en ProblemDetail con `codigo` y `correlationId`. Tiene Maven Wrapper.

| Método | Ruta | Quién lo llama | Auth |
|---|---|---|---|
| POST | `/clientes` | leads | `X-Api-Key` (clave de leads). `201` nuevo, `200` ya existía (dedup por CUIL o email) |
| GET | `/clientes/{id}` | Front, ia-resumen | Ninguna. Devuelve `ETag` |
| GET | `/clientes?telefono=&email=&q=&provincia=` | Front | Ninguna |
| PATCH | `/clientes/{id}` | Front (personal) | Exige `If-Match` con el `ETag` (`412` si cambió, `428` si falta). Acepta `X-Usuario` |
| PUT / DELETE | `/clientes/{id}/no-contactar` | Front (personal) | Ninguna |
| POST | `/clientes/{id}/anonimizacion` | Front (personal) | El email del cliente en el body como confirmación |
| GET | `/clientes/{id}/situacion-crediticia` | calificacion | `X-Api-Key` (clave de calificacion). Siempre `200` con `estado` |
| GET | `/clientes/{id}/historial` | Front | Ninguna |

- **Claves de servicio:** mínimo 32 caracteres y distintas entre sí, o el servicio no arranca. Se validan dentro de clientes, no solo en el gateway.
- **Datos personales:** es el único servicio que conoce el CUIL (cifrado AES-GCM, enmascarado en las respuestas, nunca en eventos ni logs). Una anonimización borra datos, historial y situación crediticia en una transacción.
- **Kafka que publica:** `cliente-actualizado` y `cliente-anonimizado` (sobre común, sin datos personales, clave `clienteId`). Hoy **nadie los consume**.
- **Kafka que consume:** `lead-calificado` (grupo `clientes-historial`, desde `earliest`) para el historial. Acepta JSON plano o con sobre. Los mensajes inválidos y los que fallan tras 5 reintentos van a `lead-calificado.clientes.DLT`.
- **Situación crediticia:** fuente **simulada** por ahora (el último dígito del DNI decide el resultado), con caché de 30 días en base y circuit breaker.
- **Problema detectado (2026-11-14, probado en local):** calificacion publica `fecha` como **array** (`[2026,11,14,18,56,6,49848900]`) y clientes exige texto ISO-8601, así que **todos los `lead-calificado` reales van a la DLT** y el historial queda vacío. **Corregido** en calificacion (rama `fix/fecha-lead-calificado-iso`, PR pendiente) y verificado de punta a punta. Ver [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md).

### 3.8 auth (repo `AuthMicroservice`)

Python 3.12 + FastAPI + SQLAlchemy, schema `auth` (tablas `usuarios`, `refresh_tokens`, `jwt_blacklist`; `create_all`, sin migraciones), sin Kafka. Puerto **8086** (acordado). Detalle de cómo se integra en [06-autenticacion.md](06-autenticacion.md).

| Método | Ruta | Acceso |
|---|---|---|
| POST | `/auth/login` | Público. Email + contraseña → `access_token` (JWT) + `refresh_token` |
| POST | `/auth/refresh` | Público. Rota el refresh token. `401` si el usuario fue desactivado |
| POST | `/auth/logout` | Revoca el refresh token y pone el `jti` del access token en su blacklist |
| GET | `/auth/me` | Autenticado |
| POST / GET / PATCH | `/auth/usuarios` | Solo `ADMIN`. Desactivar un usuario revoca sus refresh tokens |
| GET | `/health` | Público. `{"status":"ok"}` (el monitor lo toma como vivo) |

- **JWT:** HS256 con secreto compartido (`JWT_SECRET`, 32+ caracteres, obligatorio); claims `sub`, `email`, `rol`, `jti`, `iat`, `exp`; **15 minutos**. Refresh opaco de 7 días, guardado como hash SHA-256.
- **Roles:** `ADMIN`, `ASESOR`, `COMERCIAL`, `TELEMARKETING`.
- **Quién valida el token:** el orquestador, localmente (los demás servicios no validan JWT).
- **Corregido al integrarlo (2026-11-15):** `/auth/refresh` no chequeaba que el usuario siguiera activo; `JWT_SECRET` vacío no se validaba; faltaba el claim `email`; el puerto chocaba con leads / ia-resumen; las dependencias no estaban fijadas.
- **Límite de intentos (2026-11-15):** tras 5 fallos seguidos del mismo email, el login responde `429` con `Retry-After` y hay que esperar 5 minutos. Ver [06 §2](06-autenticacion.md).

### 3.9 scraper de multas (repo `InfraccionScrapper`)

**Estado actual:** librería y CLI de **Node** (Playwright + Cheerio, pnpm), **todavía no es un servicio**: no tiene HTTP, Kafka, `/health` ni Dockerfile.

- **Qué hace:** consulta las infracciones de **una patente** en el sitio público de la ANSV (con un Chromium real) y devuelve `{ dominio, infracciones[], subtotal, mensaje? }`. Cada acta trae `numeroActa`, `importe`, `importeSinDescuento`, `tipoInfraccion`, `lugar` y `juzgado`. El `subtotal` suma los importes sin descuento. Una patente sin multas **no es error** (`infracciones: []`).
- **Errores diferenciados:** `ErrorValidacionPatente`, `ErrorSitioNoDisponible` y `ErrorEstructuraNoReconocida` (el sitio cambió su HTML).
- **Datos personales:** las multas de una patente se tratan como dato personal: el log no incluye patente, actas ni importes.
- **Límites:** una patente por ejecución, lento (abre un navegador), depende de un sitio externo frágil. Hay que **limitar la concurrencia** para no saturarlo ni que nos bloqueen.
- **Decidido (2026-11-14):** la patente es de los **autos en stock**. vehiculos tiene que agregar el campo `patente`; hoy no lo tiene. No interviene en el flujo del lead.
- **Decidido (2026-11-14):** el nombre en el ecosistema queda **multas**. Se comunica **por Kafka** (comando y resultado), no por REST.
- **Lo que le falta para ser servicio:** ver [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md).

## 4. Mapa completo de comunicación entre servicios

### 4.1 REST sincrónico

| # | Origen → Destino | Llamada | Para qué |
|---|---|---|---|
| R1 | front → vehiculos | `GET /vehiculos`, `GET /vehiculos/{id}` | Selector y cola |
| R2 | front → leads | `POST /leads`, `GET /leads`, `GET /leads/{id}`, `PATCH /leads/{id}/state` | Preformulario y cola |
| R3 | front → calificacion | `GET /calificaciones/{leadId}`, `POST .../correcciones` | Ficha y corrección |
| R4 | front → clientes* | `GET /clientes/{id}`, `GET /clientes/{id}/historial` | Ficha |
| R5 | front → monitor | `/monitor/*` | Panel de salud |
| R6 | leads → clientes* | `POST /clientes` | Alta del cliente antes de guardar el lead |
| R7 | calificacion → vehiculos | `GET /vehiculos/{id}` | Precio para la regla de cobertura |
| R8 | calificacion → clientes* | `GET /clientes/{id}/situacion-crediticia` | Regla de financiación |
| R9 | ia-resumen → clientes* | `GET /clientes/{id}` | Nombre para el resumen |
| R10 | ia-resumen → vehiculos | `GET /vehiculos/{id}` | Marca/modelo/año |
| R11 | ia-resumen → Ollama | `POST /chat/completions` | Generar el resumen (externo) |
| R12 | monitor → todos | health checks | Monitoreo |
| R13 | clientes* → BCRA (¿directo?) | REST externo | Situación crediticia. **[A confirmar]** |
| R14 | front → auth* | `POST /auth/login` (inferido) | Login del telemarketer |
| R15 | orquestador → scraper* → vehiculos | por patente del stock (inferido) | Deudas de multas de las unidades |
| R16 | scraper* → sitios de multas | scraping externo | Fuente de las deudas |

### 4.2 Kafka asincrónico

| # | Tópico | Productor | Consumidores (grupo) | Sobre |
|---|---|---|---|---|
| K1 | `preformulario-completado` | leads | calificacion (`calificacion-group`) | Sí |
| K2 | `lead-calificado` | calificacion | leads (`leads-service`), clientes* | **No** |
| K3 | `resumen-solicitado` | leads | ia-resumen (`resumen-ia-service`) | Sí |
| K4 | `resumen-generado` | ia-resumen | leads (`leads-service`) | Sí |
| K5 | `preformulario-completado-dlt` | calificacion (error handler) | nadie | — |

### 4.3 El flujo de negocio principal (hoy, de punta a punta)

1. Front `POST /leads` → leads.
2. leads `POST /clientes` → clientes (sincrónico) → obtiene `clienteId`.
3. leads guarda el lead en `PENDIENTE` y publica **K1**. Responde 202 al front.
4. calificacion consume K1, llama a vehiculos (R7) y clientes (R8), califica, guarda y publica **K2**.
5. leads consume K2 → lead `CALIFICADO` → publica **K3**. clientes consume K2 → historial.
6. ia-resumen consume K3, llama a clientes (R9) y vehiculos (R10), genera con Ollama (R11) y publica **K4**.
7. leads consume K4 → guarda el resumen.

Hoy nadie sabe en qué paso quedó un lead si algo se cae entre medio: es exactamente el hueco que cubre el orquestador.

## 5. Inconsistencias y riesgos detectados

Conviene resolverlos (o al menos tolerarlos en el orquestador) antes de meterlo en el medio.

| # | Tema | Detalle | Impacto en el orquestador |
|---|---|---|---|
| I1 | Sobre de eventos | El doc v1.1 define `{eventId, eventType, version, occurredAt, correlationId, payload}`. leads e ia-resumen usan `timestamp` y no mandan `version`. calificacion publica `lead-calificado` **sin sobre**. | El orquestador tiene que aceptar ambos formatos y no puede contar con `eventId`/`correlationId` en K2. Correlacionar por `leadId`. |
| I2 | `motivos` en `LeadCalificado` | calificacion los manda como `List<String>` formateados; el doc y `leads/app/kafka/schemas.py` esperan objetos `{regla, puntos, descripcion}`. | Afecta a clientes* (historial). El orquestador solo debe pasarlo tal cual. |
| I3 | Campos de `PreformularioCompletado` | leads manda `montoAnticipo`, `montoPermuta`, `ubicacion`; el doc dice `anticipo`, `valorPermutaDeclarado`, `provincia`. calificacion tolera ambos. | No asumir un único nombre. |
| I4 | Ruta del cambio de estado | Doc: `PATCH /leads/{id}/estado` con body. Código: `PATCH /leads/{id}/state?nuevo_estado=`. | El proxy usa la ruta real. |
| I5 | Historial de calificaciones | El doc lo mueve a clientes, pero calificacion sigue exponiendo `GET /calificaciones/{leadId}/historial`. | Exponer solo lo que se use. |
| I6 | Casing de respuestas | leads responde en snake_case (`lead_id`, `cliente_id`); los servicios Spring en camelCase. El front normaliza. | No normalizar en el orquestador (por ahora), para no romper al front. |
| I7 | Sin gateway | El doc prevé un API Gateway (AD-10, puerto 8080) que no existe. El front llama a cada servicio. | El orquestador puede ocupar ese rol (ver doc 02). |
| I8 | leads sin `/health` | El monitor y el front caen a `/openapi.json`. | El orquestador debería hacer health checks con el mismo fallback. |
| I9 | Monitor incompleto | No sondea ia-resumen, ni clientes reales, ni auth, ni el scraper de multas. DB en H2 (se pierde al reiniciar). | Si el orquestador mide latencias, se superpone con el monitor (ver doc 02, §6). |
| I10 | ia-resumen sin API key | Llama `GET /clientes/{id}` sin `X-Api-Key`. Si clientes protege esa ruta, el resumen sale sin nombre. | Al pasar por el orquestador, este puede inyectar la clave correcta. |
| I11 | Puertos/variables viejas | `LeadsMicroservice/.env.example` tiene `CLIENT_MICROSERVICE=:1000`, `VEHICLE_MICROSERVICE=:1001`, `BCRA_API`: **no se usan en el código**. | Ignorar. |
| I12 | Kafka: mecanismo SASL | leads/ia-resumen sugieren `PLAIN`; calificacion comenta `SCRAM-SHA-256`. | Confirmar con quien administra Aiven. |
| I13 | Coreografía escondida | Que leads publique `resumen-solicitado` al recibir `lead-calificado` es lógica de flujo dentro de un servicio de dominio. | Es el primer candidato a mover al orquestador. |
| I14 | DLT solo en calificacion | leads e ia-resumen loguean y descartan mensajes con error. | El orquestador debería registrar esas fallas. |
| I15 | Monorepo | El doc (AD-08) prevé un monorepo con módulo `contracts`; en la práctica es un repo por servicio, sin contratos compartidos. | El orquestador puede publicar los esquemas de los eventos (Pydantic) como contrato de referencia. |

## 6. Patrones a copiar para el orquestador

| Necesidad | De dónde copiar | Qué tomar |
|---|---|---|
| App FastAPI + lifespan con Kafka | `LeadsMicroservice/app/main.py` | `asynccontextmanager` que arranca consumer y cierra producer |
| Config Kafka SASL_SSL (Aiven) | `LeadsMicroservice/app/kafka/config.py` | `_seguridad()` con `ssl.create_default_context(cafile=...)`, `KAFKA_ENABLED`, creación de tópicos con `AIOKafkaAdminClient` |
| Sobre de eventos | `LeadsMicroservice/app/kafka/producer.py` | `build_event_envelope()` (agregar `version`) |
| Tolerancia a Kafka caído | `IAllmFormulario/app/kafka/consumer.py` | try/except en `start_consumer` para que la API levante igual |
| Conexión Supabase | `LeadsMicroservice/app/database.py` | Pool chico (`pool_size=3`, `max_overflow=0`, `pool_pre_ping`, `pool_recycle=180`) |
| Schema propio + rol propio + migraciones | `VehiculosMicroservice` (`application.yml`, Flyway) | Schema `orquestador`, rol `orquestador_app`, migraciones versionadas (en Python: Alembic) |
| Correlation id | `VehiculosMicroservice/.../CorrelationIdFilter.java` | Leer/validar/generar `X-Correlation-Id` y devolverlo en la respuesta |
| Errores | Vehiculos / calificacion | `application/problem+json` (RFC 7807) con `codigo` y `correlationId` |
| Dockerfile Python | `LeadsMicroservice/Dockerfile` | `python:3.12-slim` + uvicorn (mejorar: usuario no root, healthcheck) |
| docker-compose con validación de variables | `VehiculosMicroservice/infra/docker-compose.yml` | `${VAR:?mensaje}` y healthcheck |
| docker-compose con Kafka local | `LeadsMicroservice/docker-compose.yml` | Broker local para desarrollo (preferir KRaft, como pide AD-03) |
| `ca.pem` | Todos los repos con Kafka | CA pública de Aiven, se puede commitear |
