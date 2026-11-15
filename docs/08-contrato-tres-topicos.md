# Contrato de mensajería: tres tópicos (tanda B)

Contrato que implementa cada repo en la tanda B. Reemplaza los 5 tópicos actuales (`preformulario-completado`, `lead-calificado`, `resumen-solicitado`, `resumen-generado` y sus DLT). Diseño y motivo: [02 §5.2](02-propuesta-orquestador.md). Plan y estado: [07 §4](07-plan-orquestacion.md).

**Regla:** ningún servicio escucha a otro. Los servicios publican **hechos** en `crm.eventos`; el orquestador decide el paso siguiente y publica **órdenes** en `crm.comandos`.

**Pase:** corte duro (decisión del 2026-11-15). Sin modo dual: cada repo cambia en su rama, se prueba todo junto en local y se mergea y despliega el mismo día. Los nombres de los tópicos van en variables de entorno.

## 1. Tópicos

| Tópico | Publica | Escucha | Grupo de consumo |
|---|---|---|---|
| `crm.comandos` | solo el orquestador | todos los servicios | uno por servicio (el de hoy); cada uno **ignora** los comandos cuyo `destino` no es él |
| `crm.eventos` | todos los servicios | solo el orquestador | `orquestador` |
| `crm.dlt` | cualquiera (mensajes que agotaron reintentos) | el orquestador | `orquestador` |

- 3 particiones, factor de replicación 1 en local. Los crea el orquestador al arrancar (Aiven no los autocrea).
- **Clave del mensaje:** el `leadId` en todo el flujo del lead (mantiene el orden por lead). `clienteId` para los eventos propios de clientes.
- Variables: `KAFKA_TOPICO_COMANDOS`, `KAFKA_TOPICO_EVENTOS`, `KAFKA_TOPICO_DLT` (o el equivalente de cada stack, ej. `crm.kafka.topicos.comandos` en Spring).

## 2. Sobre

Todos los mensajes, sin excepción, van con el sobre común. `destino` va **solo** en los comandos.

```json
{
  "eventId": "uuid",
  "eventType": "CalificarLead",
  "version": 1,
  "occurredAt": "2026-11-15T12:00:00Z",
  "timestamp": "2026-11-15T12:00:00Z",
  "correlationId": "uuid",
  "destino": "calificacion",
  "payload": { "leadId": "uuid" }
}
```

- `correlationId`: el que vino en el comando que originó el hecho. Así el flujo entero comparte uno.
- Fechas en ISO-8601 **como texto**. En Spring, el `JsonSerializer` escribe `LocalDateTime` como arreglo salvo que el campo lleve `@JsonFormat(shape = STRING)`.
- Un consumidor ignora (y loguea en debug) los `eventType` que no conoce. Nunca falla por eso.

## 3. Comandos (`crm.comandos`, los publica el orquestador)

| `eventType` | `destino` | Cuándo | Payload |
|---|---|---|---|
| `CalificarLead` | `calificacion` | Al llegar `PreformularioCompletado` | El mismo payload de `PreformularioCompletado` (§4) |
| `RegistrarCalificacion` | `clientes` | Al llegar `LeadCalificado` (automático o manual) | El mismo payload de `LeadCalificado` (§4) |
| `ActualizarCalificacion` | `leads` | Al llegar `LeadCalificado` (automático o manual) | El mismo payload de `LeadCalificado` (§4) |
| `GenerarResumen` | `ia-resumen` | Al llegar el primer `LeadCalificado` automático (una corrección manual **no** regenera el resumen) | El que hoy lleva `resumen-solicitado`: `leadId`, `clienteId`, `vehiculoId`, `anticipo`, `solicitaFinanciacion`, `patente` (opcional), `permuta{tienePermuta, descripcion, valorDeclarado}`, `ubicacion{provincia, localidad}`. El orquestador lo arma con `GET /leads/{id}` |
| `GuardarResumen` | `leads` | Al llegar `ResumenGenerado` | `leadId`, `resumen` |
| `ConsultarMultas` | `multas` | Al abrir una consulta (pedido manual o barrido; §7) y al reintentar un error reintentable del scraper | `vehiculoId`, `patente`. El orquestador la consulta a vehiculos al publicar. La clave del mensaje es el `vehiculoId` |

Qué hace cada servicio al recibirlo es lo mismo que hace hoy con el tópico viejo equivalente:

| Comando | Reemplaza a |
|---|---|
| `CalificarLead` | calificacion consumiendo `preformulario-completado` |
| `RegistrarCalificacion` | clientes consumiendo `lead-calificado` |
| `ActualizarCalificacion` | leads consumiendo `lead-calificado` (nivel, puntaje, estado `CALIFICADO`) |
| `GenerarResumen` | ia-resumen consumiendo `resumen-solicitado` |
| `GuardarResumen` | leads consumiendo `resumen-generado` |

## 4. Eventos (`crm.eventos`, los publican los servicios)

| `eventType` | Publica | Payload |
|---|---|---|
| `PreformularioCompletado` | leads | `leadId`, `clienteId`, `vehiculoId`, `montoAnticipo`, `montoPermuta`, `permutaNoComputable`, `solicitaFinanciacion`, `ubicacion`, `patente` (opcional, patente del vehículo en parte de pago tipeada por el prospecto) |
| `LeadCalificado` | calificacion | `leadId`, `clienteId`, `puntos`, `nivel`, `origen` (`AUTOMATICA` / `MANUAL`), `fecha` (ISO texto), `observacion`, `motivos[]` (igual que hoy, **ahora con sobre**) |
| `ResumenGenerado` | ia-resumen | `leadId`, `resumen` (igual que hoy) |
| `MultasConsultadas` | multas | `vehiculoId`, `dominio`, `infracciones[]` (`numeroActa`, `dominio`, `importe`, `importeSinDescuento`, `tipoInfraccion`, `lugar`, `juzgado`), `subtotal`, `mensaje` opcional. Una patente sin multas es un resultado válido (`infracciones: []`). Clave del mensaje: `vehiculoId` |
| `ConsultaMultasFallida` | multas | `vehiculoId`, `motivo` (`SITIO_NO_DISPONIBLE`, `ESTRUCTURA_NO_RECONOCIDA` o `PATENTE_INVALIDA`), `reintentable` (boolean; si falta, se trata como `false`) |
| `ClienteActualizado`, `ClienteAnonimizado` | clientes | Igual que hoy. El orquestador todavía no los usa: los ignora |

## 5. Fallas (`crm.dlt`)

- Cada servicio manda a `crm.dlt` el mensaje que agotó sus reintentos **tal como lo recibió** (con su sobre). Spring: `DeadLetterPublishingRecoverer` apuntando a `crm.dlt`. Python: publicar el sobre original.
- El orquestador lee `crm.dlt` y marca el paso según el `eventType` del mensaje fallido. Por ejemplo, `CalificarLead` en la DLT deja el flujo `FALLIDO`, como hoy `preformulario-completado-dlt`.

## 6. Qué guarda el orquestador

- El outbox guarda el comando hasta publicarlo. Los comandos que **reenvían** datos de un evento (`CalificarLead`, `RegistrarCalificacion`, `ActualizarCalificacion`, `GuardarResumen`) llevan su payload en el outbox **solo mientras están pendientes**: al publicarse (o abandonarse) el payload se borra y queda solo el `leadId`. `GenerarResumen` no guarda payload: lo arma al publicar.
- Las DLT y los eventos no se guardan: solo el estado del flujo y la idempotencia (`evento_procesado`, por `eventId`).
- **Purga:** cada hora se borran las filas del outbox ya publicadas o abandonadas (sin payload, solo ids y estado) y los eventos procesados con más de `RETENCION_HISTORIAL_DIAS` (30). Ese plazo tiene que superar la retención de Kafka, que es lo que permite que un mensaje vuelva.

## 7. Flujo de multas (`CONSULTA_MULTAS_VEHICULO`)

Las multas son de las unidades en stock y no forman parte del flujo del lead. Hay un flujo por vehículo (clave de negocio: `vehiculoId`) que se reabre en cada consulta; los pasos anteriores quedan como historial.

```
orquestador ──ConsultarMultas──► crm.comandos ──► multas (scraper)
multas ──MultasConsultadas / ConsultaMultasFallida──► crm.eventos ──► orquestador
orquestador ──PUT /vehiculos/{id}/multas (REST, desde el outbox)──► vehiculos
```

Estados: `CONSULTANDO_MULTAS` → `ACTUALIZANDO_VEHICULO` → `COMPLETADO`, o `FALLIDO` / `EXPIRADO`.

- **Quién abre una consulta:** `POST /api/orquestador/multas/{vehiculoId}` (202; 409 `CONSULTA_EN_CURSO` si ya hay una esperando) y el barrido periódico (`MULTAS_BARRIDO_HABILITADO`, apagado por defecto: pide a vehiculos `GET /vehiculos?disponible=true` y abre una consulta por unidad, hasta `MULTAS_BARRIDO_MAXIMO`). Con `MULTAS_AL_ALTA=true`, el alta de una unidad (`POST /api/vehiculos` que responde 201 con `Location`) abre su consulta en segundo plano, sin afectar la respuesta al cliente; si no, la toma el próximo barrido.
- **`ConsultaMultasFallida` reintentable:** el orquestador vuelve a pedir `ConsultarMultas` con espera `MULTAS_ESPERA_BASE_SEGUNDOS * 2^(reintento-1)` hasta `MULTAS_MAX_INTENTOS` consultas; después el flujo queda `FALLIDO`. Si no es reintentable (`ESTRUCTURA_NO_RECONOCIDA`: el sitio cambió) queda `FALLIDO` de una vez y hay que alertar.
- **`ConsultarMultas` en `crm.dlt`:** el flujo queda `FALLIDO`.
- **Sin respuesta** en `FLUJO_TIMEOUT_MULTAS_SEGUNDOS`: `EXPIRADO`; si la respuesta llega tarde, el flujo se recupera.
- **Entrega a vehiculos:** `PUT /vehiculos/{id}/multas` con el cuerpo de `MultasConsultadas` sin el `vehiculoId` y los headers `X-Api-Key` (`VEHICULOS_API_KEY`), `X-Correlation-Id` y `X-Origen-Servicio`. 2xx = entregado; 400/401/403/404/409/422 = rechazo, el flujo queda `FALLIDO` sin reintentar; cualquier otra respuesta o un corte se reintenta como cualquier comando del outbox. **vehiculos decide** qué hace con las multas.
- **Sin patente:** si `GET /vehiculos/{id}` no devuelve `patente` (o la unidad no existe), el flujo queda `FALLIDO` en el acto, sin reintentos.
- **Datos personales:** el resultado del scraper (actas, importes, lugares) no se guarda en el orquestador: viaja en el outbox solo hasta entregarse y en el log queda solo la cantidad de actas. El comando `ConsultarMultas` tampoco guarda la patente: se consulta a vehiculos al publicar.
- **Lo que le toca a cada servicio:** el scraper escucha `ConsultarMultas` (`destino: multas`), responde con `MultasConsultadas` o `ConsultaMultasFallida` y serializa las consultas (una a la vez); vehiculos agrega `patente` (migración de Flyway) y `PUT /vehiculos/{id}/multas`.
