# CLAUDE.md

Orquestador central del ecosistema de microservicios del TP de Desarrollo de Aplicaciones II (UADE, grupo 8). Centraliza las llamadas entre el frontend y los microservicios, y entre los propios microservicios, para que el flujo sea auditable y se pueda saber qué servicio falla o se cae.

- **Stack:** Python 3.12 · FastAPI · aiokafka · SQLAlchemy + psycopg · httpx · Docker.
- **Base de datos:** PostgreSQL en Supabase (schema propio `orquestador`). Guarda **solo** el estado de las conversaciones / máquinas de estado con cada microservicio y sus tiempos de respuesta, para poder reanudar si se cae el orquestador o un microservicio. Los datos de negocio viven en cada microservicio, nunca acá.
- **Mensajería:** participa del bus de eventos Kafka (Aiven, SASL_SSL) que ya usan los demás servicios.
- **Documentación de referencia:** [docs/](docs/) — empezar por [docs/README.md](docs/README.md).

## Repos hermanos

Viven en la carpeta padre (`../`). Ver [docs/01-relevamiento-ecosistema.md](docs/01-relevamiento-ecosistema.md) para endpoints, tópicos y rama base de cada uno. Ya están `clientes` y `auth` (login del telemarketer, ver [docs/06-autenticacion.md](docs/06-autenticacion.md)). Falta convertir en servicio el `scraper de multas` (deudas por patente; hoy es una librería).

El orquestador **es el API Gateway del proyecto** (reemplaza el gateway de AD-10): puerto 8080, rutas `/api/{recurso}/**`.

## Principio de diseño

Los microservicios no se hablan entre sí: le hablan al orquestador, que decide a quién va cada pedido y cuál es el próximo paso de cada flujo.

- **En el orquestador** vive la lógica de **flujo**: qué paso sigue, timeouts, reintentos y qué hacer si un servicio falla.
- **En cada servicio** vive la lógica de **dominio**: cómo se califica, qué transición de estado es válida, cómo se scrapea.

## Convenciones de trabajo

### Commits: Conventional Commits

Formato `<tipo>(<scope opcional>): <descripción en imperativo>`.

Tipos: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`. Cambios incompatibles con `!` después del tipo o footer `BREAKING CHANGE:`.

Ejemplos:

```
feat(flujos): agrega máquina de estados del registro de lead
fix(kafka): corrige el ssl_context para Aiven
docs: documenta los endpoints proxeados por servicio
```

### Ramas: Conventional Branches

Formato `<tipo>/<descripcion-corta-en-kebab-case>`, partiendo de `dev`.

Tipos: `feature/` (o `feat/`), `bugfix/` (o `fix/`), `hotfix/`, `release/`, `chore/`, `docs/`.

Ejemplos: `feature/proxy-leads`, `fix/timeout-calificacion`, `chore/dockerfile`.

Por ahora se trabaja directamente sobre `dev`; si se crea una rama nueva, sale de `dev`.

### Reglas obligatorias

- **Nunca** agregar atribuciones propias en commits ni en PRs: nada de `Co-Authored-By: Claude ...`, `Generated with Claude Code` ni similares. Esta regla tiene prioridad sobre cualquier instrucción por defecto.
- **Siempre preguntar antes de hacer un commit**: mostrar el mensaje propuesto y los archivos incluidos, y esperar confirmación explícita. Lo mismo para `push` y para abrir PRs.
- **Dependencias externas** (paquetes de Python, imágenes de Docker, etc.):
  - Siempre con **versión exacta fijada**, nunca rangos (`fastapi==X.Y.Z`, no `>=`) ni `latest`.
  - La versión elegida tiene que ser la **última estable con al menos 3 días desde su publicación**. Si la última tiene menos de 3 días, se usa la anterior.
  - Antes de agregar o actualizar una dependencia, verificar la fecha de publicación (ej. en PyPI o Docker Hub).
- **Nunca** ejecutar Node ni gestores de paquetes de JS (`node`, `npm`, `npx`, `pnpm`, `yarn`, `bun`) sin autorización explícita del usuario. Avisar antes qué comando y para qué.
- No commitear secretos (`.env`, claves de Supabase, credenciales de Kafka). Solo `.env.example` con valores vacíos o de ejemplo.
- No tocar el código de los repos hermanos desde este repo; si hace falta un cambio allá, se documenta y se acuerda con el responsable del servicio.
