# Herramientas de desarrollo

Nada de esta carpeta se despliega: es solo para el entorno local.

| Carpeta | Qué es |
|---|---|
| [`clientes_simulado/`](clientes_simulado/main.py) | Simulador del microservicio `clientes`. **Opcional:** si levantás el servicio real (`ClientMicroservice`), no lo uses. Sirve para probar leads o calificacion sin levantar Java. Corre en el compose con `docker compose --profile clientes-simulado up -d` (puerto 8082, el mismo que el real: uno u otro). |
