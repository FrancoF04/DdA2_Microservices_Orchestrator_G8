"""Qué rutas del gateway son públicas y qué puede llamar cada servicio.

Es la tabla que decide el acceso (docs/06-autenticacion.md §3). Todo lo que no está
acá como público es protegido: ante la duda, se pide autenticación.
"""

from __future__ import annotations

from collections.abc import Mapping

# Recursos (primer segmento de /api/{recurso}) a los que puede llamar cada servicio con su
# credencial. Es el mínimo que usan hoy; ampliar solo cuando un flujo lo necesite.
RECURSOS_POR_SERVICIO: dict[str, frozenset[str]] = {
    # leads da de alta el cliente antes de guardar el lead.
    "leads": frozenset({"clientes"}),
    # calificacion consulta el vehículo y la situación crediticia del cliente.
    "calificacion": frozenset({"clientes", "vehiculos"}),
    # ia-resumen arma el resumen con el nombre del cliente y los datos del vehículo.
    "ia-resumen": frozenset({"clientes", "vehiculos"}),
}

_LECTURA = {"GET", "HEAD"}
_AUTH_PUBLICAS = {"/auth/login", "/auth/refresh", "/auth/logout"}


def es_publica(metodo: str, recurso: str, ruta: str, headers: Mapping[str, str]) -> bool:
    """`ruta` es la del servicio destino (sin el prefijo /api), ej. `/vehiculos/123`."""
    metodo = metodo.upper()
    base = ruta.rstrip("/") or "/"

    # El preflight de CORS no lleva credenciales; lo responde el middleware de CORS.
    if metodo == "OPTIONS":
        return True
    # Preformulario del prospecto: no tiene login.
    if recurso == "leads" and metodo == "POST" and base == "/leads":
        return True
    # Catálogo de vehículos (datos y fotos): lo ve cualquiera.
    if recurso == "vehiculos" and metodo in _LECTURA:
        return True
    # Altas y cambios de vehículos (scripts de carga): los valida vehiculos con su clave.
    if recurso == "vehiculos" and "x-api-key" in {h.lower() for h in headers}:
        return True
    # Login y renovación: se piden justamente porque todavía no hay token válido.
    if recurso == "auth" and metodo == "POST" and base in _AUTH_PUBLICAS:
        return True
    return False
