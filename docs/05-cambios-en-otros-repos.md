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
| `crm.comandos` (orquestador → servicios) | `preformulario-completado` (lado consumidor), `resumen-solicitado` |
| `crm.eventos` (servicios → orquestador) | `preformulario-completado` (lado productor), `lead-calificado`, `resumen-generado` |
| `crm.dlt` | `preformulario-completado-dlt` |

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

## Por repo

### LeadsMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Orquestación completa | Pasar a `crm.comandos` / `crm.eventos` (ver arriba). Deja de publicar `resumen-solicitado`: ese paso lo decide el orquestador. | Que el flujo viva en el orquestador. |
| 🟡 | Agregar `GET /health` (ej. `{"status": "UP"}`). | Hoy el monitor y el front caen a `/openapi.json` para saber si está vivo. |
| 🟡 | Mandar `X-Origen-Servicio: leads` en la llamada a clientes. | Que el orquestador sepa quién llama (hoy figura `desconocido`). |
| ⚪ | En `.env.example`: usar `postgresql+psycopg://` en `DATABASE_URL` y sacar `CLIENT_MICROSERVICE`, `VEHICLE_MICROSERVICE` y `BCRA_API`, que no se usan. | Evitar confusiones al levantarlo. |
| ⚪ | Fijar versiones en `requirements.txt`. | Hoy instala la última de cada paquete. |

### CalificacionMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Orquestación completa | Pasar a `crm.comandos` / `crm.eventos`. Consumir el comando `CalificarLead` en vez de `preformulario-completado`. | Ídem. |
| 🟡 | Publicar `lead-calificado` **con el sobre común** (hoy va como JSON plano). | Hoy se pierde el `correlationId` en ese paso: no se puede seguir un flujo de punta a punta con un solo ID. Verificado en local. |
| 🟡 | `motivos` como objetos `{regla, puntos, descripcion}` en vez de texto formateado. | Es lo que dice el contrato y lo que necesita el historial de clientes. |
| 🟡 | Mandar `X-Origen-Servicio: calificacion` en las llamadas a vehiculos y clientes. | Trazabilidad. |

### IAllmFormulario (ia-resumen)

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Ecosistema en Docker / nube | Agregar un `Dockerfile`. | Es el único servicio sin imagen. |
| 🔴 Orquestación completa | Consumir el comando `GenerarResumen` de `crm.comandos` y publicar `ResumenGenerado` en `crm.eventos`. | Ídem. |
| 🟡 | Si no consigue el cliente o el vehículo, generar el resumen igual con lo que tenga (o publicar un error). | Verificado en local: si vehiculos no responde, el consumer revienta con un error de validación y el resumen no se genera nunca, sin avisar a nadie. |
| 🟡 | Mandar `X-Origen-Servicio: ia-resumen` en las llamadas. | Trazabilidad. |
| ⚪ | Mover `pytest` a un `requirements-dev.txt`. | Hoy se instala en producción. |

### VehiculosMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Flujo de multas | Agregar el campo `patente` (migración de Flyway, la hace el responsable). | El scraper de multas consulta por patente. |
| 🔴 Flujo de multas | Recibir el resultado del scraper: `PUT /vehiculos/{id}/multas` o comando por `crm.comandos`, a definir. | Que vehiculos decida qué hacer con las multas. |

### MonitorMicroservice

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🟡 | Sumar como objetivos al **orquestador** (`/health`) y a **ia-resumen** (`/openapi.json`). Después, a auth, clientes real y multas. | Hoy no los vigila. |
| ⚪ | Usar Postgres en vez de H2 en memoria (ya lo soporta por variables). | Hoy pierde el historial en cada reinicio. |

**No cambiar:** el monitor tiene que seguir sondeando a cada servicio **directo**, no a través del orquestador.

### DDA2 (front)

| Prioridad | Cambio | Para qué |
|---|---|---|
| 🔴 Pasar el tráfico por el orquestador | Apuntar las `VITE_*_API_URL` al orquestador (`<orq>/api`). | Sin esto el front sigue llamando a cada servicio directo. |
| 🟡 | Rehacer el sondeo de respaldo de la página Monitor (`src/api/monitorClient.js`). | Con las URLs nuevas deja de funcionar. Opciones en [03-despliegue-render.md §4](03-despliegue-render.md#nota-sondeo-de-respaldo-del-front). |
| ⚪ | Unificar las `VITE_*_API_URL` en una sola (`VITE_API_URL`, que ya existe). | Con el gateway todas valen lo mismo. |

### Servicios nuevos (clientes, auth, scraper de multas)

Al crearlos conviene que nazcan ya con:

- `GET /health` (o Actuator) y un `Dockerfile`.
- Kafka con el esquema de 3 tópicos, si lo usan.
- Errores en formato ProblemDetail y `X-Correlation-Id` propagado.
- Header `X-Origen-Servicio` en las llamadas que hagan.
- **clientes:** los 4 endpoints que ya usan los demás. El simulador de este repo ([dev/clientes_simulado](../dev/clientes_simulado/main.py)) muestra exactamente qué devuelve cada uno.
