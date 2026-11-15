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
| `GenerarResumen` | `ia-resumen` | Al llegar el primer `LeadCalificado` automático (una corrección manual **no** regenera el resumen) | El que hoy lleva `resumen-solicitado`: `leadId`, `clienteId`, `vehiculoId`, `anticipo`, `solicitaFinanciacion`, `permuta{tienePermuta, descripcion, valorDeclarado}`, `ubicacion{provincia, localidad}`. El orquestador lo arma con `GET /leads/{id}` |
| `GuardarResumen` | `leads` | Al llegar `ResumenGenerado` | `leadId`, `resumen` |

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
| `PreformularioCompletado` | leads | `leadId`, `clienteId`, `vehiculoId`, `montoAnticipo`, `montoPermuta`, `permutaNoComputable`, `solicitaFinanciacion`, `ubicacion` (igual que hoy) |
| `LeadCalificado` | calificacion | `leadId`, `clienteId`, `puntos`, `nivel`, `origen` (`AUTOMATICA` / `MANUAL`), `fecha` (ISO texto), `observacion`, `motivos[]` (igual que hoy, **ahora con sobre**) |
| `ResumenGenerado` | ia-resumen | `leadId`, `resumen` (igual que hoy) |
| `ClienteActualizado`, `ClienteAnonimizado` | clientes | Igual que hoy. El orquestador todavía no los usa: los ignora |

## 5. Fallas (`crm.dlt`)

- Cada servicio manda a `crm.dlt` el mensaje que agotó sus reintentos **tal como lo recibió** (con su sobre). Spring: `DeadLetterPublishingRecoverer` apuntando a `crm.dlt`. Python: publicar el sobre original.
- El orquestador lee `crm.dlt` y marca el paso según el `eventType` del mensaje fallido. Por ejemplo, `CalificarLead` en la DLT deja el flujo `FALLIDO`, como hoy `preformulario-completado-dlt`.

## 6. Qué guarda el orquestador

- El outbox guarda el comando hasta publicarlo. Los comandos que **reenvían** datos de un evento (`CalificarLead`, `RegistrarCalificacion`, `ActualizarCalificacion`, `GuardarResumen`) llevan su payload en el outbox **solo mientras están pendientes**: al publicarse (o abandonarse) el payload se borra y queda solo el `leadId`. `GenerarResumen` no guarda payload: lo arma al publicar.
- Las DLT y los eventos no se guardan: solo el estado del flujo y la idempotencia (`evento_procesado`, por `eventId`).
