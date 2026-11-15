# Documentación del orquestador

| Documento | Contenido |
|---|---|
| [01-relevamiento-ecosistema.md](01-relevamiento-ecosistema.md) | Estado actual de cada repo: rama base, stack, endpoints que expone, llamadas que hace a otros servicios, tópicos Kafka e inconsistencias detectadas. |
| [02-propuesta-orquestador.md](02-propuesta-orquestador.md) | Qué tráfico pasa a manejar el orquestador, separado por repo; rol frente a Kafka; modelo de la base de datos; máquina de estados; dockerización y plan por fases. |
| [03-despliegue-render.md](03-despliegue-render.md) | Despliegue en Render, CORS y qué variable cambia en cada repo para que su tráfico pase por el orquestador. |
| [04-desarrollo-local.md](04-desarrollo-local.md) | Levantar todo el ecosistema en local: infraestructura en Docker y cada servicio nativo, con el `.env` de cada repo. |
| [05-cambios-en-otros-repos.md](05-cambios-en-otros-repos.md) | Cambios pendientes en cada repo (Kafka de 3 tópicos, `/health`, headers, etc.), con su prioridad. |
| [06-autenticacion.md](06-autenticacion.md) | Login de los operadores (JWT validado en el orquestador), rutas públicas y protegidas, credenciales de servicio, identidad hacia los servicios, configuración y límites conocidos. |
| [07-plan-orquestacion.md](07-plan-orquestacion.md) | Plan de la orquestación real por tandas (outbox, tres tópicos, flujo de multas) y su estado. |
| [08-contrato-tres-topicos.md](08-contrato-tres-topicos.md) | Contrato de mensajería: tópicos, sobre, comandos, eventos, DLT y el flujo de multas. |
| [09-despliegue-desde-cero.md](09-despliegue-desde-cero.md) | Despliegue completo desde cero en Aiven, Supabase y Render: Blueprint de los 10 servicios, secretos, SQL de roles y verificación de punta a punta. |

Relevamiento hecho el 2026-11-14 sobre la rama local de cada repo (sin `git fetch`). Si los repos avanzan, hay que revalidar.
