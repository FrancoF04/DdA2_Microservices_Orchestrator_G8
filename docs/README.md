# Documentación del orquestador

| Documento | Contenido |
|---|---|
| [01-relevamiento-ecosistema.md](01-relevamiento-ecosistema.md) | Estado actual de cada repo: rama base, stack, endpoints que expone, llamadas que hace a otros servicios, tópicos Kafka e inconsistencias detectadas. |
| [02-propuesta-orquestador.md](02-propuesta-orquestador.md) | Qué tráfico pasa a manejar el orquestador, separado por repo; rol frente a Kafka; modelo de la base de datos; máquina de estados; dockerización y plan por fases. |
| [03-despliegue-render.md](03-despliegue-render.md) | Despliegue en Render, CORS y qué variable cambia en cada repo para que su tráfico pase por el orquestador. |
| [04-desarrollo-local.md](04-desarrollo-local.md) | Levantar todo el ecosistema en local: infraestructura en Docker y cada servicio nativo, con el `.env` de cada repo. |
| [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md) | Cambios pendientes en cada repo (Kafka de 3 tópicos, `/health`, headers, etc.), con su prioridad. |

Relevamiento hecho el 2026-11-14 sobre la rama local de cada repo (sin `git fetch`). Si los repos avanzan, hay que revalidar.
