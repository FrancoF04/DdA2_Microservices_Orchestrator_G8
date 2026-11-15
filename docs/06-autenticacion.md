# Autenticación

Cómo se controla el acceso al ecosistema. Todo pasa por el orquestador (el API Gateway), así que **es el único punto de control**: los microservicios no validan JWT ni se hablan entre sí sin pasar por él.

## 1. Cómo funciona, en una mirada

```
 prospecto (sin login) ──► POST /api/leads, GET /api/vehiculos/**  ──┐
                                                                     │
 operador (telemarketer/admin) ── Authorization: Bearer <JWT> ───────┤
                                                                     ▼
                                                           ┌──────────────────┐   X-Usuario: ana@...
 leads / calificacion / ia-resumen                         │   ORQUESTADOR    │ ───────────────────────► servicios
   X-Origen-Servicio + X-Clave-Servicio ──────────────────►│ valida el JWT    │   (no ven el JWT ni la
                                                           │ localmente       │    clave del servicio)
                                                           └────────┬─────────┘
                                          POST /api/auth/login ─────┘ (solo login, refresh y logout)
                                          POST /api/auth/refresh      llegan a AuthMicroservice
```

- **Quién se loguea:** los operadores internos (roles `ADMIN`, `ASESOR`, `COMERCIAL`, `TELEMARKETING`). El prospecto que carga el preformulario **no tiene login**.
- **Quién emite los tokens:** `AuthMicroservice` (puerto 8086). Guarda usuarios y contraseñas (bcrypt) en su schema `auth`.
- **Quién los valida:** el orquestador, **localmente**, con la firma HS256 (mismo secreto). No llama a auth en cada pedido: si auth se cae, los operadores que ya tienen sesión siguen trabajando.
- **Entre servicios:** cada servicio que llama a otro por el orquestador se identifica con su propia credencial (§4).

## 2. Operadores: login, token, refresh y logout

| Paso | Qué pasa |
|---|---|
| **Login** `POST /api/auth/login` | Email y contraseña → `access_token` (JWT, **15 minutos**) y `refresh_token` (opaco, 7 días). Tras **5 fallos seguidos** del mismo email responde `429` con el header `Retry-After` (300 s) y hay que esperar **5 minutos** antes de volver a probar; durante ese tiempo ni la contraseña correcta entra. Un login correcto borra los fallos. El front puede leer `Retry-After` (el orquestador lo expone en CORS). |
| **Cada pedido** | `Authorization: Bearer <access_token>`. El orquestador verifica firma, vencimiento y que no esté revocado. |
| **Vence el access token** | El orquestador responde `401` con `codigo: TOKEN_EXPIRADO`. El front llama a `POST /api/auth/refresh` con el refresh token, recibe un par nuevo y repite el pedido. El operador no se entera. |
| **Refresh** `POST /api/auth/refresh` | Rota el refresh token (el viejo deja de servir). Falla si el usuario fue desactivado. |
| **Logout** `POST /api/auth/logout` | Revoca el refresh token **y el access token en el acto**: el orquestador ve el logout, guarda el `jti` en su tabla `token_revocado` y ese token responde `401 TOKEN_REVOCADO` desde ese momento (también después de reiniciar el orquestador). |

El access token lleva los claims `sub` (id), `email`, `rol`, `jti`, `iat` y `exp`.

Códigos de error del gateway (siempre en `application/problem+json`, con `WWW-Authenticate: Bearer` en los 401):

| `codigo` | HTTP | Cuándo |
|---|---|---|
| `NO_AUTENTICADO` | 401 | Falta el token (o el header está mal armado). |
| `TOKEN_EXPIRADO` | 401 | El token venció: renovarlo con `/api/auth/refresh`. |
| `TOKEN_INVALIDO` | 401 | Firma incorrecta, algoritmo no permitido o falta un claim (`exp`, `sub`, `jti`). |
| `TOKEN_REVOCADO` | 401 | La sesión se cerró con logout. |
| `CLAVE_SERVICIO_INVALIDA` | 401 | Credencial de servicio equivocada o servicio desconocido. |
| `SERVICIO_NO_AUTORIZADO` | 403 | El servicio es válido pero no puede llamar a ese recurso. |

El `429` del login lo responde auth (con su propio cuerpo `{"detail": "..."}`); el orquestador lo deja pasar tal cual, con `Retry-After`.

## 3. Qué es público y qué está protegido

La tabla está en [`app/seguridad/politica.py`](../app/seguridad/politica.py). **Todo lo que no figura como público pide autenticación.**

| Público (sin login) | Protegido (operador logueado o servicio autorizado) |
|---|---|
| `POST /api/leads` (el preformulario) | `GET /api/leads`, `GET /api/leads/{id}`, `PATCH /api/leads/{id}/state` |
| `GET /api/vehiculos/**` (datos y fotos) | `/api/calificaciones/**` (ver y corregir) |
| `POST /api/auth/login`, `/refresh`, `/logout` | `/api/clientes/**` (ficha, edición, historial, anonimización) |
| `POST/PUT/PATCH/DELETE /api/vehiculos/**` **con `X-Api-Key`** (scripts de carga) | `/api/monitor/**`, `/api/resumen*`, `/api/multas/**` |
| Preflight CORS (`OPTIONS`) | `/api/orquestador/**` (flujos y métricas) y `GET /api/auth/me`, `/api/auth/usuarios/**` |
| `/health`, `/docs`, `/openapi.json` (rutas propias, fuera de `/api`) | Un recurso que no existe (`/api/xyz`): 401, no 404, para no revelar qué hay |

Por ahora **cualquier operador logueado** accede a todo lo protegido (decisión del equipo). `AuthMicroservice` sí aplica el rol `ADMIN` en `/auth/usuarios/**`. Las escrituras de vehículos con `X-Api-Key` pasan sin JWT porque las valida `vehiculos` con su clave de escritura.

## 4. Servicios: credencial ante el orquestador

Los servicios que llaman a otros a través del orquestador no tienen JWT. Se identifican con dos headers:

```
X-Origen-Servicio: ia-resumen
X-Clave-Servicio: <su clave, 32+ caracteres>
```

El orquestador compara la clave en tiempo constante contra `CLAVES_SERVICIO` y **solo deja pasar a los recursos que ese servicio usa**:

| Servicio | Puede llamar a | Para qué |
|---|---|---|
| `leads` | `clientes` | Alta del cliente antes de guardar el lead. |
| `calificacion` | `clientes`, `vehiculos` | Situación crediticia y datos de la unidad. |
| `ia-resumen` | `clientes`, `vehiculos` | Nombre del cliente y datos del vehículo para el resumen. |

- `X-Clave-Servicio` **no se reenvía** al destino (es un secreto entre el servicio y el orquestador). La `X-Api-Key` que valida el destino (clientes, vehiculos) sí sigue de largo: hay **dos capas**.
- Declarar `X-Origen-Servicio` sin la clave no alcanza (`401`).
- Con credencial válida, la auditoría registra el **origen verificado** (`leads`, `calificacion`, `ia-resumen`). Para las llamadas del operador registra `front`.
- Para sumar un servicio o ampliar sus permisos: `RECURSOS_POR_SERVICIO` en `politica.py` y una entrada en `CLAVES_SERVICIO`.

## 5. Identidad hacia los servicios

- Si el pedido trae un JWT válido, el orquestador agrega **`X-Usuario`** (el email) y **`X-Usuario-Rol`**.
- **Descarta cualquier `X-Usuario` / `X-Usuario-Rol` que venga de afuera**, en rutas públicas y protegidas: nadie puede hacerse pasar por otro operador. Un valor con caracteres raros (saltos de línea, no ASCII) no se inyecta; se usa el id del usuario.
- El JWT **solo se reenvía a auth**. Los demás servicios no lo ven.
- `clientes` ya lee `X-Usuario` para registrar quién operó. `calificacion` lo usa como autor de la **corrección manual**, por encima del campo `usuario` del body.

## 6. Configuración

| Dónde | Variable | Qué es |
|---|---|---|
| Orquestador | `JWT_SECRET` | El mismo que auth. 32+ caracteres. **Sin él, el orquestador no arranca** (no puede quedar abierto). |
| Orquestador | `CLAVES_SERVICIO` | `leads=<clave>,calificacion=<clave>,ia-resumen=<clave>`. Cada una de 32+ caracteres y distinta. |
| Orquestador | `AUTH_HABILITADA` | `true` por defecto. `false` solo en desarrollo y tests. |
| Orquestador | `JWT_ALGORITHM` | `HS256` (también `HS384`/`HS512`). Nunca `none` ni asimétricos. |
| Orquestador | `AUTH_URL` | `http://<auth>:8086`. |
| Auth | `JWT_SECRET` | El mismo que el orquestador. 32+ caracteres o no arranca. |
| Auth | `JWT_ACCESS_TOKEN_EXPIRE_MINUTES` | 15 por defecto. |
| leads, calificacion, ia-resumen | `ORQUESTADOR_CLAVE_SERVICIO` | Su clave en `CLAVES_SERVICIO`. Obligatoria si sus URLs apuntan al orquestador; vacía si llaman directo. |

Valores de desarrollo en [04-desarrollo-local.md](04-desarrollo-local.md). En Render, `JWT_SECRET` y `CLAVES_SERVICIO` van como secretos (`sync: false` en [`render.yaml`](../render.yaml)); nunca en el repo.

## 7. Cómo se verificó (2026-11-15)

Con auth, orquestador, clientes, vehiculos, calificacion, leads e ia-resumen nativos:

- El prospecto carga el preformulario y ve el catálogo **sin token**; leads da de alta al cliente por el orquestador con su credencial.
- Todo lo del operador responde `401 NO_AUTENTICADO` sin token y `200` con token.
- El flujo `REGISTRO_LEAD` corre completo con los servicios llamándose **solo por el orquestador**; la auditoría muestra `leads`, `calificacion` e `ia-resumen` como orígenes verificados.
- Corrección manual con un `usuario` falso en el body: queda a nombre del email del token.
- Logout: el token se invalida en el acto, las otras sesiones siguen, y el refresh token cerrado da 401. **El logout sobrevive al reinicio del orquestador.**
- Un telemarketer ve la cola pero auth le niega `/auth/usuarios` (403). Al desactivarlo: no puede renovar ni volver a loguearse.
- Credencial equivocada, servicio sin permiso, origen declarado sin clave: 401 / 403 / 401.

Tests: 163 en el orquestador (más de 100 son de autenticación, incluida una persistencia contra Postgres), 42 en auth, 19 en calificacion.

## 8. Límites conocidos

| Límite | Detalle | Qué haría falta |
|---|---|---|
| **Usuario desactivado** | Pierde el refresh y el login en el acto, pero su access token ya emitido vale hasta 15 minutos. | Aceptarlo, o que el orquestador revoque por `sub` cuando ve el `PATCH /api/auth/usuarios/{id}`. |
| **Revocaciones con varias réplicas** | Cada réplica conoce los logouts que pasaron por ella hasta reiniciar. | Consultar la tabla en cada pedido o un caché compartido. Con una réplica no pasa. |
| **Secreto compartido (HS256)** | Quien conozca `JWT_SECRET` puede firmar tokens: hay que tratarlo como secreto en Render y rotarlo si se filtra. | JWT asimétrico (RS256): auth firma con la privada y el orquestador solo necesita la pública. |
| **Límite de intentos de login** | Se cuenta por email, en la memoria de auth: con varias réplicas cada una cuenta por su lado, y un reinicio lo borra. Además, alguien que sepa el email de otro puede bloquearlo 5 minutos a propósito. | Guardar los contadores en la base o en un caché compartido. |
| **Refresh token robado** | Se rota en cada uso, pero reusar uno viejo no cierra toda la sesión. | Detección de reuso. |
| **Todos los operadores pueden todo** | Hoy cualquier operador logueado accede a `/api/monitor` y `/api/orquestador`. | Reglas por rol (`X-Usuario-Rol` ya llega). |
| **Se cae auth** | Los operadores con sesión siguen; fallan login y refresh. El logout igual se aplica en el orquestador. | — |

## 9. Pendiente

- **Front (DDA2):** pantalla de login, guardar los tokens, mandar `Authorization`, renovar cuando llega `TOKEN_EXPIRADO` y reintentar el pedido, y logout. Sin esto el front recibe 401 en la cola.
- **Monitor:** sumar a auth (`/health`) como objetivo.
- **Nube:** definir `JWT_SECRET` y `CLAVES_SERVICIO` en Render, y las `ORQUESTADOR_CLAVE_SERVICIO` en cada servicio.
