# Plan de orquestación real (paso 3): tanda A y tanda B

Documento de traspaso: alcanza para retomar el trabajo en una conversación nueva. Complementa a [02-propuesta-orquestador.md](02-propuesta-orquestador.md) (diseño completo) y [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md) (qué toca en cada repo).

## 1. Dónde estamos (2026-11-15)

Hecho y mergeado (PR hechas por el equipo):

| Qué | Repo |
|---|---|
| Proxy `/api/{recurso}/**`, seguimiento de flujos (`REGISTRO_LEAD`), infra local, clientes real | orquestador |
| Autenticación completa: JWT validado en el gateway, credenciales de servicio, logout inmediato, límite de intentos de login | orquestador, auth, leads, ia-resumen, calificacion |
| Login del operador en el front; el front pasa todo por el orquestador | DDA2 |

Hasta la tanda A el orquestador **observa** el flujo (consume los tópicos y avanza la máquina de estados) pero **no dirige** nada: leads publica `resumen-solicitado` por su cuenta. La tanda A (§3.5) agrega el despachador, apagado por defecto.

## 2. Decisiones tomadas

- **Tanda A primero, tanda B después** (decisión del usuario, 2026-11-15).
- **leads deja de publicar `resumen-solicitado`**; lo publica el orquestador con el mismo contrato (aceptado).
- **Una corrección manual de la calificación NO regenera el resumen de la IA.** Motivo: el resumen de ia-resumen no depende de la calificación. `IAllmFormulario/app/kafka/consumer.py` lo arma solo con datos del lead (nombre y apellido del cliente, marca/modelo/año del vehículo, anticipo, permuta, ubicación, financiación). No usa nivel, puntaje ni motivos. Si algún día el resumen pasa a incluir la calificación, hay que revisar esta decisión.
- Reglas del equipo: una rama por fase; el usuario abre las PR; no se commitea ni se pushea sin pedirlo; pnpm únicamente y pedir permiso antes de correr Node; dependencias con versión exacta y al menos 3 días de publicadas; respuestas en español; **ningún commit ni PR lleva líneas de atribución**; "todos somos responsables de todos los servicios" (se pueden tocar repos hermanos en rama propia y documentarlo en docs/05).

## 3. Tanda A: el orquestador publica `resumen-solicitado` (Fase 2)

### 3.1 Flujo objetivo

```
leads --preformulario-completado--> [orquestador: flujo REGISTRO_LEAD]
calificacion --lead-calificado--> [orquestador]
                                    |  publica resumen-solicitado   <-- NUEVO
                                    v
ia-resumen --resumen-generado--> leads (guarda el resumen) y orquestador (cierra el paso)
```

El contrato de `resumen-solicitado` no cambia (`ResumenSolicitado` con sobre `{eventId, eventType, correlationId, timestamp, payload}`, `payload.leadId` y los datos comerciales). ia-resumen no se entera del cambio.

### 3.2 Tareas en el orquestador (rama `feature/orquestacion-tanda-a`)

1. **Datos del comando.** Al recibir `lead-calificado`, armar el payload de `resumen-solicitado`. Recomendación de docs/02: **`GET /leads/{id}` por el propio gateway** (con la credencial de servicio del orquestador) en lugar de guardar los datos comerciales en el flujo. Ver el payload real que hoy publica leads en `LeadsMicroservice/app/kafka` para respetar el contrato.
2. **Outbox.** Ya existe el modelo `Outbox` en `app/db/models.py`. Escribir el comando en la tabla y publicarlo desde un despachador (publicación confiable: si Kafka está caído no se pierde). Reintentos con tope y marca de error.
3. **Máquina de estados.** Pasar el flujo a `RESUMEN_PENDIENTE` cuando el comando queda publicado, y cerrarlo con `resumen-generado`. Mantener los timeouts existentes.
4. **Reintento manual.** `POST /api/orquestador/flujos/{id}/reintentar` (protegido: lo exige el router de flujos). Vuelve a publicar `resumen-solicitado`; leads es idempotente.
5. **Identidad en llamadas salientes.** El orquestador es un servicio más: si llama por HTTP a leads u otros, usar la credencial correspondiente (ver `app/seguridad/`).
6. **Healthcheck propio.** Ya existe `app/api/health.py`; confirmar que el monitor puede sondearlo.
7. **Tests con TDD** y comprobación de que fallan si se rompe el comportamiento (mutación manual). Migración Alembic si cambia el esquema, con `alembic check` limpio.
8. **Documentación:** actualizar docs/02 (decisiones abiertas 2 y 4 resueltas), docs/05, docs/04 y este documento.

### 3.3 Repos hermanos (ramas chicas, base `main`)

| Repo | Rama sugerida | Cambio |
|---|---|---|
| LeadsMicroservice | `feat/resumen-lo-publica-el-orquestador` | Quitar `publish_resumen_solicitado` del consumer de `lead-calificado` (o dejarlo detrás de una variable, por defecto apagado). Agregar `GET /health`. Actualizar tests y README. |
| MonitorMicroservice | `feat/objetivos-orquestador-auth-ia` | Sumar como objetivos monitoreados: orquestador (`/health`), ia-resumen (`/openapi.json`) y auth (`/health` o el que tenga). Ver cómo se configuran los objetivos (`crm.monitor.servicios`). |

Orden de despliegue: primero el orquestador con el despachador **apagado por defecto** o leads con la variable que mantiene el comportamiento viejo; después se invierte. Así nunca hay dos publicadores ni ninguno. Verificar que ia-resumen tolera un `resumen-solicitado` repetido.

### 3.4 Prueba de punta a punta

Levantar con docs/04: infra, orquestador, auth, leads, clientes, vehiculos, calificacion e ia-resumen (para el resumen hace falta `GOOGLE_API_KEY` o similar; ver el README de ia-resumen; sin clave el consumer falla al generar). Enviar un preformulario, ver el flujo en `/api/orquestador/flujos`, confirmar que **solo** el orquestador publica `resumen-solicitado` (kafka-ui en `:8090`) y que el resumen llega al lead.

### 3.5 Estado de la tanda A (implementada el 2026-11-15, sin commitear)

Hecho en las tres ramas; los tres repos con cambios sin commit hasta que el usuario lo autorice.

| Tarea del §3.2 | Cómo quedó |
|---|---|
| 1. Datos del comando | `GET /leads/{id}` **directo a `LEADS_URL`** al momento de publicar (no por el propio gateway: leads no exige credenciales en sus endpoints, la capa de auth es el gateway, y evita que el orquestador se llame a sí mismo). Va con `X-Origen-Servicio: orquestador` y el correlation id del flujo. Armado del payload en `app/flujos/resumen.py` (mismo contrato que publicaba leads). |
| 2. Outbox | `app/flujos/despachador.py`. Al ver `lead-calificado` se anota una fila en `outbox` en la **misma transacción** que avanza el flujo; un despachador la publica aparte. La fila guarda solo `eventType`, `leadId` y `correlationId` (no datos de negocio). Reintentos con espera `base * 2^(n-1)` (5 s, tope 300 s) y tope de `OUTBOX_MAX_INTENTOS=5`; al agotarlos la fila queda `abandonado_en` y el flujo `FALLIDO`. Sin Kafka conectado no se gastan intentos. Varios despachadores no se pisan (`SKIP LOCKED`). Migración `0005` (`alembic check` limpio). |
| 3. Máquina de estados | Al publicar, el despachador abre el paso `GENERAR_RESUMEN` y pasa el flujo a `RESUMEN_PENDIENTE`; el eco del propio comando que lee el consumidor es idempotente. Un pedido nuevo tras un `TIMEOUT` abre otro intento del paso (`intento = 2`). Los timeouts no cambiaron. |
| 4. Reintento manual | `POST /api/orquestador/flujos/{id}/reintentar` (202; 404 si no existe; 409 `FLUJO_COMPLETADO`, `FLUJO_SIN_CALIFICAR`, `FLUJO_NO_REINTENTABLE` o `DESPACHADOR_APAGADO`). Lo exige el router de flujos (operador logueado). Si ya hay un pedido pendiente no lo duplica. |
| 5. Identidad saliente | `X-Origen-Servicio: orquestador` (ver arriba); no hay clave de servicio porque no pasa por el gateway. |
| 6. Healthcheck | `/health` ya sirve: responde 200 con `status`; el monitor lo marca DOWN si la base o Kafka reportan `DOWN`. |
| 7. Tests | 36 tests nuevos (`test_flujo_resumen_pedido.py`, `test_outbox_integracion.py`); 200 en total contra Postgres. Se comprobó por mutación manual que fallan al romper cada comportamiento (13 mutaciones, todas detectadas). |

**Interruptores (por defecto, nada cambia al mergear):**

| Variable | Dónde | Por defecto | Efecto |
|---|---|---|---|
| `ORQUESTADOR_PUBLICA_RESUMEN` | orquestador | `false` | `true` enciende el outbox y el despachador. |
| `LEADS_PUBLICA_RESUMEN` | leads | `true` | `false` apaga el publicador viejo. |

**Orden de pase (nunca una ventana sin publicador):**

1. Mergear y desplegar las tres ramas (nada cambia: orquestador observa, leads publica).
2. Encender `ORQUESTADOR_PUBLICA_RESUMEN=true`. Por un momento publican los dos: ia-resumen genera el resumen dos veces y leads guarda el último (inofensivo).
3. Apagar `LEADS_PUBLICA_RESUMEN=false` en leads.
4. Cuando esté estable, quitar el código viejo de leads (queda como mejora).

**Pendientes de la tanda A:**

- Prueba de punta a punta con ia-resumen real (necesita la clave del LLM; ver el README de ia-resumen). Verificado de punta a punta con Kafka real y un leads simulado (orquestador real): un solo `resumen-solicitado` con el contrato de siempre y el correlation id del flujo, flujo hasta `COMPLETADO`, reintento sobre un flujo completado → 409.
- Las filas publicadas del outbox no se purgan (solo guardan ids; falta una retención como la de `auditoria_retencion_dias`).
- Si leads está caído mucho tiempo (más de ~75 s con los valores por defecto) el flujo queda `FALLIDO`; se recupera con el reintento manual.

## 4. Tanda B: tres tópicos (implementada el 2026-11-15, sin commitear)

Decisiones del usuario: **corte duro** (sin modo dual: cada repo cambia en su rama, se prueba todo junto y se despliega el mismo día) y **contrato + orquestador primero**. El contrato está en [08-contrato-tres-topicos.md](08-contrato-tres-topicos.md).

| Repo | Rama | Qué cambia |
|---|---|---|
| orquestador | `feature/orquestacion-tanda-b` | Lee `crm.eventos` y `crm.dlt`; por cada evento ordena el paso siguiente en `crm.comandos` (outbox, con `destino`). Los comandos que reenvían datos guardan su payload en el outbox **solo mientras están pendientes**. Los de un mismo lead salen en orden (`outbox.orden`, migración 0006). Se quita `ORQUESTADOR_PUBLICA_RESUMEN`. |
| leads | `feat/tres-topicos-crm` | Publica `PreformularioCompletado`; atiende `ActualizarCalificacion` y `GuardarResumen`; DLT tras 3 intentos. Ya no publica `resumen-solicitado` ni escucha otros servicios. |
| calificacion | `feat/tres-topicos-crm` | Atiende `CalificarLead`; publica `LeadCalificado` con sobre y con el `correlationId` del comando; DLT copiando el mensaje como texto. |
| ia-resumen | `feat/tres-topicos-crm` | Atiende `GenerarResumen`; publica `ResumenGenerado`; DLT tras 3 intentos. |
| clientes | `feat/tres-topicos-crm` | Atiende `RegistrarCalificacion`; sus eventos salen por `crm.eventos`; su DLT pasa a `crm.dlt`. |

**Flujo resultante** (ningún servicio escucha a otro):

```
leads ──PreformularioCompletado──► crm.eventos ──► orquestador ──CalificarLead──► crm.comandos ──► calificacion
calificacion ──LeadCalificado──► crm.eventos ──► orquestador ──► RegistrarCalificacion (clientes)
                                                              ├─► ActualizarCalificacion (leads)
                                                              └─► GenerarResumen (ia-resumen)
ia-resumen ──ResumenGenerado──► crm.eventos ──► orquestador ──GuardarResumen──► leads
```

**Pase a Aiven (hacerlo con el flujo quieto, de una vez):** el plan free permite 5 tópicos y los 5 viejos ocupan el cupo.

1. Esperar a que no haya flujos en curso (`GET /api/orquestador/flujos?estado=...`) o aceptar reprocesarlos con el reintento manual.
2. Borrar los 5 tópicos viejos en Aiven (`preformulario-completado`, `lead-calificado`, `resumen-solicitado`, `resumen-generado`, `preformulario-completado-dlt`; y `lead-calificado.clientes.DLT` si existe).
3. Mergear las 5 ramas y desplegar. El orquestador crea `crm.comandos`, `crm.eventos` y `crm.dlt` al arrancar (si el usuario de Aiven no puede crear tópicos, crearlos a mano). Los nombres van en `KAFKA_TOPICO_*`.

**Pendientes de la tanda B:**

- Prueba de punta a punta con los cinco servicios reales y ia-resumen con su clave del LLM.
- Los payloads de `GuardarResumen` incluyen el texto del resumen (puede nombrar al cliente): está en el outbox solo hasta publicarse; si se quisiera evitar, guardar solo el `leadId` y pedir el resumen a leads.
- Después de B: flujo de multas (ver §4.1).

### 4.1 Tanda C: flujo de multas y purga del outbox

**Orquestador (rama `feature/flujo-multas-y-purga-outbox`):**

- Flujo `CONSULTA_MULTAS_VEHICULO` completo: `ConsultarMultas` por Kafka, reintentos con espera creciente, entrega del resultado a vehiculos por `PUT /vehiculos/{id}/multas` desde el outbox, vencimiento y recuperación. Contrato en docs/08 §7.
- `POST /api/orquestador/multas/{vehiculoId}` y barrido periódico (apagado por defecto).
- Purga horaria del outbox resuelto y de `evento_procesado` (`RETENCION_HISTORIAL_DIAS`, 30).
- Los pasos de `GET /api/orquestador/flujos` exponen `intento`.
- 26 tests nuevos con Postgres (241 en total) y 8 mutaciones manuales detectadas. De punta a punta con Kafka y Postgres reales (scraper real con la ANSV simulada y vehiculos falso): éxito, sitio caído con reintento, cambio de estructura sin reintento y patente sin multas.

**InfraccionScrapper (rama `feat/servicio-kafka-multas`):** ya es el servicio `multas` (kafkajs 2.2.4, solo pnpm): atiende `ConsultarMultas`, responde `MultasConsultadas` / `ConsultaMultasFallida`, una consulta a la vez con pausa, DLT, `/health`, log sin datos personales. 43 tests y 10 mutaciones detectadas. Falta su `Dockerfile` (imagen de Playwright) y probarlo contra la ANSV real (`pnpm exec playwright install chromium`).

**Falta, en otros repos:**

- vehiculos (Naim): migración de `patente` y `PUT /vehiculos/{id}/multas` (qué hace con las multas lo decide vehiculos).
- Que el alta de una unidad abra la consulta sola (hoy la toma el barrido): requiere leer el cuerpo de la respuesta del proxy.
- Activar `MULTAS_BARRIDO_HABILITADO` y definir `VEHICULOS_API_KEY` cuando vehiculos tenga `patente`.

## 5. Otros pendientes (fuera de A y B)

- Auth: detección de reutilización del refresh token, Dockerfile endurecido, migraciones, RS256 (docs/06 §8).
- Front: probar el login con clics en el navegador (no se pudo probar solo); que `/monitor` pase a `/api/monitor` y pida login si se despliega el orquestador.
- ClientMicroservice: el README indica `-pl clientes-service -am spring-boot:run`, que falla (instalar `contracts` y correr solo el módulo del servicio).
- Nube (Render): definir `JWT_SECRET`, `CLAVES_SERVICIO` y la `ORQUESTADOR_CLAVE_SERVICIO` de cada servicio.
- `DDA2/package-lock.json` convive con `pnpm-lock.yaml`; podría sobrar.

## 6. Cómo retomar en una conversación nueva

1. Leer la memoria `estado-y-proximos-pasos` y este documento.
2. `git status` y `git log` en el orquestador y en los repos de §3.3 para ver qué ramas existen.
3. Entorno local: docs/04 (infra en Docker, servicios nativos; `127.0.0.1` en Windows; el orquestador se arranca con `python -m app`).
4. Seguir el §3.2 en orden, con TDD. Pedir permiso antes de commitear, y dar título y descripción de PR por repo al terminar.
