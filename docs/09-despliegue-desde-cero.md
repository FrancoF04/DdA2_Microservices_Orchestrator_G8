# Despliegue desde cero (Aiven + Supabase + Render)

Cómo borrar todo lo que haya desplegado y levantar el ecosistema completo de nuevo. Reemplaza a [03](03-despliegue-render.md) para el despliegue (03 sigue valiendo para entender CORS y el plan free).

**Qué se levanta:** 10 servicios en Render (orquestador, auth, leads, calificacion, clientes, vehiculos, ia-resumen, multas, monitor y el front), un servicio Kafka en Aiven con los 3 tópicos del contrato ([08](08-contrato-tres-topicos.md)) y un schema por servicio en Supabase.

**Las piezas, en este repo:**

| Pieza | Para qué |
|---|---|
| [`render.yaml`](../render.yaml) | Blueprint con los 10 servicios, cada uno desde su repo. Un solo "New → Blueprint" los crea a todos |
| [`scripts/preparar_despliegue.py`](../scripts/preparar_despliegue.py) | Genera los secretos compartidos y arma las variables de cada servicio y el SQL de roles |
| [`infra/supabase/`](../infra/supabase/) | `00` muestra qué hay, `01` borra lo del CRM, `02` crea un rol y un schema por servicio |
| [`scripts/verificar_despliegue.py`](../scripts/verificar_despliegue.py) | Prueba el ecosistema desplegado de punta a punta (lead y multas) |

**Ensayado en local de punta a punta (2026-11-15):** un Postgres vacío con los roles de `02` (como Supabase), un Kafka vacío **sin autocreación de tópicos** (como Aiven) y los 9 servicios con exactamente las variables que salen de `render.yaml` y del preparador (solo cambian las URLs, el host de la base y Kafka sin SASL). Los servicios crearon los 3 tópicos solos y `verificar_despliegue.py` dio todo OK: login, los 8 servicios respondiendo, un lead hasta `COMPLETADO` (con el LLM simulado) y las multas de AAA111 consultadas en la ANSV real. Aparte: los SQL contra un "despliegue anterior" simulado (borran lo del CRM y no tocan `auth` de Supabase) y el servicio multas con 512 MB de límite (~290 MiB de pico en una consulta real).

El ensayo encontró dos cosas que ya están corregidas: el email del admin no puede tener un dominio reservado como `.local` (auth lo rechaza y no se podría entrar), y clientes solo acepta letras en nombre y apellido.

---

## 1. Antes de empezar (en local)

1. **Mergear todo** lo que tenga que ir. Render construye desde `main` en todos los repos menos el orquestador, que construye desde `dev` (su `main` quedó en el commit inicial). Para cambiarlo, editar `branch` en `render.yaml`.
2. **Juntar los datos de los proveedores** (secciones 2 y 3) y correr el preparador:

   ```bash
   python scripts/preparar_despliegue.py   # la 1ra vez crea despliegue.local/proveedores.env y termina
   # completar despliegue.local/proveedores.env
   python scripts/preparar_despliegue.py   # genera secretos, variables por servicio y el SQL de roles
   ```

   Deja en `despliegue.local/` (está en `.gitignore`, **nunca subirlo**):
   - `secretos.env`: JWT, credenciales entre servicios, claves de los roles, cifrado del CUIL. Se generan **una vez**: volver a correr el script no los cambia. Guardar una copia: si se pierden `CLIENTES_CUIL_CLAVE`/`_HMAC`, los CUIL guardados quedan ilegibles.
   - `variables-por-servicio.md`: un bloque `.env` por servicio con todas sus variables secretas.
   - `supabase-roles.sql`: el SQL de roles con las claves reales.

   El script también verifica que cada variable `sync: false` del `render.yaml` tenga valor.

**Las claves que tienen que coincidir** (el preparador ya las arma iguales):

| Clave | Dónde va |
|---|---|
| `JWT_SECRET` | auth y orquestador |
| Clave de cada servicio ante el gateway | `CLAVES_SERVICIO` del orquestador (`leads=…,calificacion=…,ia-resumen=…`) y `ORQUESTADOR_CLAVE_SERVICIO` de cada uno |
| `CLIENTES_CLAVE_LEADS`, `CLIENTES_CLAVE_CALIFICACION` | clientes y quien lo llama (leads, calificacion) |
| `VEHICULOS_CLAVE_ESCRITURA` | vehiculos y `VEHICULOS_API_KEY` del orquestador |
| Credenciales de Aiven | todos los que usan Kafka (orquestador, leads, calificacion, clientes, ia-resumen, multas) |

---

## 2. Aiven (Kafka)

1. **No borrar el proyecto**, solo el servicio Kafka (o sus tópicos). El certificado `ca.pem` que tienen los repos es la CA **del proyecto** (vence en 2036): mientras sea el mismo proyecto sigue sirviendo. Si se crea un proyecto nuevo, hay que reemplazar `ca.pem` en los 6 repos que lo usan (orquestador, leads, ia-resumen, calificacion, `clientes-service/` de clientes y multas).
2. Borrar el servicio Kafka viejo (o todos sus tópicos: el plan free admite solo 5).
3. Crear el servicio Kafka (plan free) y habilitar **SASL** (Service settings → Advanced configuration → `kafka_authentication_methods.sasl`). De **Connection information**, pestaña SASL, anotar en `proveedores.env`:
   - `AIVEN_BOOTSTRAP`: host y **puerto SASL** (no el de SSL con certificado de cliente).
   - `AIVEN_SASL_MECANISMO`: `SCRAM-SHA-256` (el de Aiven por defecto; `PLAIN` si así está configurado).
   - `AIVEN_USUARIO` y `AIVEN_CLAVE` (por defecto `avnadmin`).
4. Tópicos: `crm.comandos`, `crm.eventos` y `crm.dlt` (3 particiones). Los crean los servicios al arrancar; si el usuario no puede crearlos, crearlos a mano en la consola de Aiven con esos nombres.

## 3. Supabase (PostgreSQL)

1. En **SQL Editor**, correr [`infra/supabase/00-ver-que-hay.sql`](../infra/supabase/00-ver-que-hay.sql) y revisar qué schemas y roles hay.
   - Si aparecen roles o schemas del CRM con otros nombres (de un despliegue anterior, por ejemplo `orquestador_app`), agregarlos a la lista de `01` antes de seguir.
   - **No borrar `auth`**: es el schema del servicio de autenticación de Supabase. Por eso el auth del CRM usa el schema y el rol `crm_auth`. Si un despliegue anterior dejó tablas del CRM dentro de `auth`, borrarlas una por una.
2. Correr [`01-borrar-todo.sql`](../infra/supabase/01-borrar-todo.sql) (**destructivo**: borra los schemas y roles del CRM con sus datos).
3. Correr `despliegue.local/supabase-roles.sql` (el `02` con las claves reales). Crea `orquestador`, `crm_auth`, `leads`, `calificacion`, `clientes` y `vehiculos`: cada rol es dueño de su schema y lo tiene como `search_path`. Se puede volver a correr.
4. De **Project Settings → Database → Connection string → Session pooler** anotar en `proveedores.env`: `SUPABASE_REF` (el project-ref) y `SUPABASE_POOLER_HOST` (ej. `aws-0-sa-east-1.pooler.supabase.com`). Los servicios entran por el **session pooler, puerto 5432**, con el usuario `<rol>.<project-ref>` (el preparador ya arma así las URLs). La conexión directa es solo IPv6 y Render no la alcanza.

Las tablas las crea cada servicio al arrancar: el orquestador con Alembic, clientes y vehiculos con Flyway, calificacion, leads y auth con Hibernate/SQLAlchemy. El monitor no usa la base (H2 en memoria) y ia-resumen y multas tampoco.

## 4. Render

1. **Borrar todos los servicios viejos** (y los environment groups que hubiera) del workspace.
2. Correr de nuevo `python scripts/preparar_despliegue.py` con `proveedores.env` completo.
3. **New → Blueprint**, elegir este repo y la rama `dev`. Render lee `render.yaml` y muestra los 10 servicios.
4. Render pide las variables `sync: false` **una sola vez**, ahora: copiarlas de `despliegue.local/variables-por-servicio.md` (un bloque por servicio). Si se escapa alguna, después se carga en el servicio → Environment → *Add from .env* (Render no la vuelve a pedir).
5. Crear. El primer build tarda: los Java compilan con Maven y la imagen de multas pesa ~3,7 GB.
6. **Revisar las URLs.** El `render.yaml` supone `https://<name>.onrender.com`. Si Render le agregó un sufijo a alguno (porque el nombre estaba tomado), actualizar su URL en:

| Servicio | Variables que tienen su URL |
|---|---|
| orquestador | `CLIENTES_URL` de leads; `CLIENTES_SERVICE_URL` y `VEHICULOS_SERVICE_URL` de calificacion; `CLIENTES_URL` y `VEHICULOS_URL` de ia-resumen; `ORQUESTADOR_SERVICE_URL` del monitor; `VITE_API_URL` del front |
| front | `CORS_ALLOWED_ORIGINS` del orquestador, monitor, calificacion, clientes y vehiculos; `FRONTEND_LIST` de auth y leads |
| cualquier otro | su URL en el orquestador (`<SERVICIO>_URL`), en el monitor (`<SERVICIO>_SERVICE_URL`) y, si tiene, en el front (`VITE_*`) |

   Las `VITE_*` del front se leen al **compilar**: después de cambiarlas hay que hacer *Manual Deploy → Clear build cache & deploy*.

## 5. Verificar

```bash
python scripts/verificar_despliegue.py \
    --gateway https://dda2-g8-orquestador.onrender.com \
    --monitor https://dda2-g8-monitor.onrender.com
```

Despierta todo por el monitor, revisa el `/health` del orquestador (base y Kafka), entra con el admin, mira el estado de cada servicio según el monitor, y después prueba los dos flujos de punta a punta:

- **Lead:** un preformulario nuevo hasta `COMPLETADO` (leads → clientes → calificacion → ia-resumen con el LLM real). Con `--sin-llm` alcanza con que quede calificado.
- **Multas:** la unidad con patente `AAA111` (tiene actas reales en la ANSV) hasta `COMPLETADO` (scraper → vehiculos).

Toma el usuario y la clave del admin de `despliegue.local/`. Termina con `TODO OK` o con la lista de pasos que fallaron, y el detalle de cada paso dice en qué servicio se trabó.

## 6. Datos iniciales

- **Vehículos:** el perfil `seed` (ya puesto en `SPRING_PROFILES_ACTIVE` de vehiculos) carga 10 unidades con patente de prueba. Es idempotente: se puede dejar puesto.
- **Admin:** auth crea el usuario `SEED_ADMIN_EMAIL` con `SEED_ADMIN_PASSWORD` al arrancar (están en `despliegue.local/`). Es el usuario para entrar al front.

## 7. Prender el flujo de multas automático

Con todo arriba y el paso 5 en verde, en el orquestador → Environment:

- `MULTAS_BARRIDO_HABILITADO=true`: consulta las multas de todas las unidades disponibles una vez por día (la primera, al arrancar).
- `MULTAS_AL_ALTA=true`: el alta de una unidad abre su consulta.

Arrancan apagadas a propósito. Un consumidor de Kafka que se une por primera vez empieza a leer desde ese momento. Si el barrido publicara antes de que el servicio multas haya arrancado, esas consultas se perderían y los flujos quedarían EXPIRADO.

## 8. Opcional: fotos de los vehículos

Están apagadas (`VEHICULOS_FOTOS_MODO=deshabilitado`; el seed no tiene fotos). Para prenderlas: crear un bucket público en Supabase Storage y, en vehiculos, poner `VEHICULOS_FOTOS_MODO=supabase`, `VEHICULOS_FOTOS_BUCKET`, `VEHICULOS_SUPABASE_URL` y `VEHICULOS_SUPABASE_CLAVE_SECRETA` (ver el README de vehiculos).

## 9. El plan free en la práctica

| Tema | Qué pasa | Qué hacer |
|---|---|---|
| **Se duermen** | A los 15 min sin tráfico HTTP entrante. Kafka **no** los despierta: un servicio dormido no procesa sus comandos hasta despertarse | El monitor sondea a todos cada 15 s, y mientras él está despierto (alguien usa el front) los mantiene despiertos. Antes de una demo: abrir el front o correr el paso 5 |
| **Horas** | 750 por mes para **todo** el workspace. Con los 10 servicios despiertos son unas 75 horas de uso completo por mes | Dejar que se duerman cuando nadie los usa. No poner un "pinger" externo 24/7 |
| **Primer pedido** | Un servicio dormido tarda hasta ~1 min en responder | El orquestador espera 60 s (`PROXY_TIMEOUT_SEGUNDOS`) |
| **Memoria** | 512 MB por servicio | Los Java van con `-XX:MaxRAMPercentage=70` (en `render.yaml`). Multas: ~290 MiB de pico por consulta |
| **Red privada** | Los servicios free no reciben tráfico privado | Todos se llaman por URL pública (`https://…onrender.com`) |

## 10. Problemas frecuentes

| Síntoma | Causa probable | Arreglo |
|---|---|---|
| Un servicio no pasa el health check al desplegar | Variable faltante o mal copiada (el log del servicio dice cuál) | Revisar sus variables contra `variables-por-servicio.md` |
| `password authentication failed` | El usuario no tiene `.<project-ref>` o la clave no es la de `supabase-roles.sql` | Volver a correr `supabase-roles.sql` (actualiza las claves) |
| `permission denied for schema` / tablas en `public` | El rol no es dueño de su schema | Correr de nuevo `supabase-roles.sql` |
| Kafka: `SaslAuthenticationException` o timeouts | Puerto SSL en vez del SASL, o mecanismo distinto al del usuario | `AIVEN_BOOTSTRAP` con el puerto SASL; `AIVEN_SASL_MECANISMO` igual al de Aiven |
| El front carga pero todo da error de CORS | La URL real del front no es la del `render.yaml` | Sección 4.6 |
| Login OK pero las pantallas dan 401 | `JWT_SECRET` distinto en auth y orquestador | Copiar el de `secretos.env` en los dos |
| Un lead queda en `CALIFICACION_PENDIENTE` | calificacion dormido o con Kafka mal configurado | Despertarlo (monitor) y revisar sus `SPRING_KAFKA_*` |
| Un lead queda en `RESUMEN_PENDIENTE` | ia-resumen sin `OLLAMA_API_KEY` o dormido | Revisar su clave; reintentar con `POST /api/orquestador/flujos/{id}/reintentar` |
| Multas `FALLIDO` "no tiene patente" | La unidad no tiene patente | Cargarla con `PUT /api/vehiculos/{id}` |

## 11. Lo que queda fuera de este despliegue

No impide que el ecosistema funcione:

- **Front:** no hay pantalla de multas ni de flujos; falta probar el login con clics en el navegador; `/monitor` debería pasar por `/api/monitor`.
- **Auth:** detección de reutilización del refresh token y Dockerfile endurecido.
- **clientes:** sus tests con base de datos fallan en Windows (ver su README); el comando `-pl clientes-service -am spring-boot:run` del README falla.
- **ia-resumen** tiene su propio `render.yaml` (de antes): ya no se usa, el Blueprint de este repo lo reemplaza.
