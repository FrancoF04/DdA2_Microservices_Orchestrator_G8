# Herramientas de desarrollo

Nada de esta carpeta se despliega: es solo para el entorno local.

| Carpeta | Qué es |
|---|---|
| [`clientes_simulado/`](clientes_simulado/main.py) | Simulador del microservicio `clientes`, que todavía no existe. Sin él falla `POST /leads`. Corre en el compose (`clientes-simulado`, puerto 8082). Cuando esté el servicio real: `docker compose stop clientes-simulado`. |
