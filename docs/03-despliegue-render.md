# Despliegue en Render y conexión de los servicios

> **Para desplegar desde cero, seguir [09-despliegue-desde-cero.md](09-despliegue-desde-cero.md)** (Blueprint de los 10 servicios, secretos y verificación). Este documento sigue valiendo para entender CORS, el plan free y por qué cada servicio apunta al gateway.

Cómo se despliega el orquestador en Render (la plataforma que usa el resto del equipo) y qué cambia en cada repo para que su tráfico pase por él.

## 1. Crear el servicio en Render

1. En Render: **New → Blueprint**, elegir este repo. Render lee [`render.yaml`](../render.yaml) y crea el servicio web `orquestador` con runtime Docker.
2. Completar en el dashboard las variables marcadas `sync: false`:

| Variable | Valor |
|---|---|
| `CORS_ALLOWED_ORIGINS` | URL del front en Render (ej. `https://dda2-front.onrender.com`). Separar con comas si hay más de una. |
| `LEADS_URL`, `CLIENTES_URL`, `CALIFICACION_URL`, `MONITOR_URL`, `IA_RESUMEN_URL`, `AUTH_URL`, `MULTAS_URL` | URL pública de cada servicio en Render, **sin** el recurso (ej. `https://leadsmicroservice.onrender.com`). Vacía = el orquestador responde `503 SERVICIO_NO_CONFIGURADO`. |
| `DATABASE_URL` | Session pooler de Supabase, puerto **5432**: `postgresql://orquestador_app.<project-ref>:<clave>@<host-pooler>:5432/postgres?sslmode=require` |
| `KAFKA_*` | Credenciales de Aiven. |

3. Health check: Render usa `GET /health`. Responde 200 mientras el proceso viva, aunque Kafka o la base estén caídos (en ese caso informa `DEGRADADO`). Así Render no reinicia el orquestador por una dependencia ajena.

Las migraciones (`alembic upgrade head`) se aplican solas en cada arranque.

## 2. Particularidades de Render (plan free)

| Tema | Qué pasa | Cómo lo maneja el orquestador |
|---|---|---|
| **Servicios que se duermen** | Tras ~15 min sin tráfico, un servicio tarda hasta ~1 min en responder el primer pedido. | `PROXY_TIMEOUT_SEGUNDOS=60`. Si igual vence, responde `504 SERVICIO_TIMEOUT` y reinicia el cliente HTTP de ese servicio (ver abajo). Para despertar todo antes de una demo: `POST /api/monitor/despertar`. |
| **Cadena de despertares** | Un `POST /api/leads` con todo dormido despierta orquestador → leads → clientes, uno detrás de otro. | Cada salto tiene su propio timeout de 60 s. El front debería tolerar una primera respuesta lenta o reintentar. |
| **Pool trabado tras un arranque en frío** | Observado en pruebas: después de un timeout contra un servicio de Render que estaba despertando, todas las requests siguientes a ese servicio vencían, mientras un cliente nuevo respondía en 0,5 s. | Un cliente HTTP **por servicio** (uno trabado no afecta a los demás) que **se reemplaza** después de cada timeout. |
| **Proxy de Render adelante** | Las requests llegan con `X-Forwarded-For/Proto`. | `FORWARDED_ALLOW_IPS=*` para que uvicorn los acepte. |
| **Conexión a Supabase** | La conexión directa de Supabase es solo IPv6. | Usar el **session pooler** (IPv4), puerto 5432. |
| **Puerto** | Render define `PORT`. | El entrypoint lo usa (`${PORT:-8080}`). |

## 3. CORS

Con el orquestador como gateway, **solo el orquestador** necesita CORS: el navegador ya no habla con los servicios.

- El origen del front va en `CORS_ALLOWED_ORIGINS` del orquestador.
- El orquestador **quita** los headers `Access-Control-*` que manden los servicios, para no duplicarlos (el navegador rechaza la respuesta si llegan dos).
- Las consultas previas (`OPTIONS` con `Access-Control-Request-Method`) las responde el orquestador y no se reenvían.
- La config de CORS de cada servicio se puede dejar como está; deja de usarse.

## 4. Qué cambia en cada repo (fase 1a: solo variables de entorno)

En todos los casos `<orq>` es la URL pública del orquestador en Render, por ejemplo `https://orquestador.onrender.com`.

| Repo | Variable | Valor nuevo | Comentario |
|---|---|---|---|
| `DDA2` (front) | `VITE_LEADS_API_URL`, `VITE_VEHICULOS_API_URL`, `VITE_CALIFICACION_API_URL`, `VITE_CLIENTES_API_URL`, `VITE_MONITOR_API_URL` | `<orq>/api` | Ver nota sobre el sondeo de respaldo abajo. |
| `LeadsMicroservice` | `CLIENTES_URL` | `<orq>/api` | El `X-Api-Key` sigue igual: el orquestador lo reenvía. |
| `CalificacionMicroservice` | `VEHICULOS_SERVICE_URL`, `CLIENTES_SERVICE_URL` | `<orq>/api` | Ídem. |
| `IAllmFormulario` | `VEHICULOS_URL`, `CLIENTES_URL` | `<orq>/api` | — |
| `MonitorMicroservice` | — | **sin cambios** | Debe seguir sondeando a cada servicio directo, para distinguir "se cayó el orquestador" de "se cayó un servicio". Opcional: agregar el orquestador (`/health`) e ia-resumen como objetivos. |
| `VehiculosMicroservice` | — | sin cambios | Solo recibe. Los scripts de carga pueden apuntar a `<orq>/api` si se quiere auditar. |

Funciona porque cada servicio arma su URL como `{BASE}/{recurso}/...` (ej. leads hace `f"{CLIENTES_URL}/clientes"`). Con `BASE=<orq>/api` queda `<orq>/api/clientes`, que el orquestador reenvía a `clientes:/clientes`.

**Recomendado (opcional, cambio de una línea):** que cada servicio mande el header `X-Origen-Servicio: <nombre>` en sus llamadas. Sin él, el orquestador registra el origen como `desconocido` (el front se detecta solo, por el header `Origin`).

### Nota: sondeo de respaldo del front

`DDA2/src/api/monitorClient.js`, cuando el monitor no responde, sondea cada servicio directo usando las mismas `VITE_*_API_URL` (`{url}/actuator/health`, `{url}/health`...). Con esas variables apuntando al orquestador, ese respaldo pega a `<orq>/api/actuator/health`, que no existe, y muestra los servicios como degradados. Opciones (a decidir con el equipo del front):

1. Que el respaldo use `GET <orq>/api/orquestador/servicios` (estado según el tráfico real).
2. Que el respaldo use variables propias con las URLs directas de cada servicio.

## 5. Probar que funciona

```bash
curl https://<orq>/health
curl "https://<orq>/api/vehiculos?disponible=true"
curl "https://<orq>/api/orquestador/servicios?ventana_minutos=60"
```

El último muestra, por servicio: cantidad de llamadas, % de error, latencia p50/p95 y el último error.

## 6. Riesgo abierto: los servicios siguen siendo públicos

Entre servicios de Render las llamadas van por URL pública. Cualquiera puede llamar directo a un servicio sin pasar por el orquestador, así que "todo pasa por el orquestador" es una convención, no algo forzado. Para forzarlo haría falta que cada servicio acepte solo pedidos con una clave que tenga el orquestador, o usar la red privada de Render (hay que confirmar si está disponible en el plan free). Queda como decisión del equipo; no bloquea la fase 1.
