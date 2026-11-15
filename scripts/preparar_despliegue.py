"""Prepara un despliegue desde cero: genera los secretos compartidos y arma las variables de cada servicio.

Uso (desde la raíz del repo):

    python scripts/preparar_despliegue.py

La primera vez crea `despliegue.local/proveedores.env` para completar con lo que dan Aiven, Supabase y Ollama, y
termina. La segunda vez (ya completado) genera, sin pisar los que ya existan:

- `despliegue.local/secretos.env`: las claves que se inventan acá (JWT, credenciales entre servicios, claves de los
  roles de la base, cifrado del CUIL). Se generan UNA vez y se reusan: regenerarlas rompe lo que ya está desplegado
  (en especial el CUIL cifrado de clientes).
- `despliegue.local/variables-por-servicio.md`: un bloque `.env` por servicio con todas sus variables `sync: false`
  del render.yaml, listo para pegar en Render ("Add from .env").
- `despliegue.local/supabase-roles.sql`: infra/supabase/02-crear-roles-y-schemas.sql con las claves reales.

También verifica que cada variable `sync: false` del render.yaml tenga valor. `despliegue.local/` está en
.gitignore: nunca subirlo.
"""

from __future__ import annotations

import base64
import secrets
import sys
from pathlib import Path
from typing import Callable

import yaml

RAIZ = Path(__file__).resolve().parent.parent
SALIDA = RAIZ / "despliegue.local"
PROVEEDORES = SALIDA / "proveedores.env"
SECRETOS = SALIDA / "secretos.env"

PLANTILLA_PROVEEDORES = """\
# Datos de los proveedores para el despliegue (docs/09-despliegue-desde-cero.md). NO subir a git.

# --- Aiven (servicio Kafka > Overview > Connection information, pestaña SASL) ---
AIVEN_BOOTSTRAP=<host>:<puerto-sasl>
# El mecanismo SASL del usuario: SCRAM-SHA-256 (el de Aiven por defecto) o PLAIN.
AIVEN_SASL_MECANISMO=SCRAM-SHA-256
AIVEN_USUARIO=avnadmin
AIVEN_CLAVE=

# --- Supabase (Project Settings > Database > Connection string > Session pooler) ---
# El <project-ref> del proyecto (el que aparece en la URL del dashboard).
SUPABASE_REF=
# El host del session pooler, ej. aws-0-sa-east-1.pooler.supabase.com (puerto 5432).
SUPABASE_POOLER_HOST=

# --- Ollama (clave de la API del LLM de ia-resumen) ---
OLLAMA_API_KEY=

# --- Usuario administrador que crea auth al arrancar ---
ADMIN_EMAIL=admin@dda2-crm.com
"""

# Roles de la base (deben coincidir con infra/supabase/02-crear-roles-y-schemas.sql).
ROLES_DB = {
    "orquestador": "CLAVE_DB_ORQUESTADOR",
    "crm_auth": "CLAVE_DB_AUTH",
    "leads": "CLAVE_DB_LEADS",
    "calificacion": "CLAVE_DB_CALIFICACION",
    "clientes": "CLAVE_DB_CLIENTES",
    "vehiculos": "CLAVE_DB_VEHICULOS",
}


DOMINIOS_RESERVADOS = ("local", "localhost", "test", "invalid", "example", "onion", "arpa")


def _clave() -> str:
    """48 caracteres URL-safe: sirve para todas las credenciales (todas piden 32 o más)."""
    return secrets.token_urlsafe(36)


def _base64_32() -> str:
    """32 bytes en base64: lo que pide clientes para el cifrado (AES-256-GCM) y el HMAC del CUIL."""
    return base64.b64encode(secrets.token_bytes(32)).decode()


GENERADORES: dict[str, Callable[[], str]] = {
    "JWT_SECRET": _clave,
    "CLAVE_ORQ_LEADS": _clave,
    "CLAVE_ORQ_CALIFICACION": _clave,
    "CLAVE_ORQ_IA_RESUMEN": _clave,
    "CLIENTES_CLAVE_LEADS": _clave,
    "CLIENTES_CLAVE_CALIFICACION": _clave,
    "VEHICULOS_CLAVE_ESCRITURA": _clave,
    "CLIENTES_CUIL_CLAVE": _base64_32,
    "CLIENTES_CUIL_HMAC": _base64_32,
    "ADMIN_PASSWORD": _clave,
    **{clave: _clave for clave in ROLES_DB.values()},
}


def leer_env(ruta: Path) -> dict[str, str]:
    valores: dict[str, str] = {}
    if not ruta.exists():
        return valores
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        nombre, _, valor = linea.partition("=")
        valores[nombre.strip()] = valor.strip()
    return valores


def asegurar_secretos() -> dict[str, str]:
    """Lee los secretos ya generados y agrega los que falten (nunca regenera uno existente)."""
    actuales = leer_env(SECRETOS)
    nuevos = {nombre: generar() for nombre, generar in GENERADORES.items() if not actuales.get(nombre)}
    if nuevos:
        with SECRETOS.open("a", encoding="utf-8") as archivo:
            if not actuales:
                archivo.write("# Secretos del despliegue. Generados UNA vez: no regenerar ni subir a git.\n")
            for nombre, valor in nuevos.items():
                archivo.write(f"{nombre}={valor}\n")
        print(f"Secretos nuevos: {', '.join(nuevos)}")
    return {**actuales, **nuevos}


def _faltantes_proveedores(prov: dict[str, str]) -> list[str]:
    obligatorios = ["AIVEN_BOOTSTRAP", "AIVEN_SASL_MECANISMO", "AIVEN_USUARIO", "AIVEN_CLAVE", "SUPABASE_REF",
                    "SUPABASE_POOLER_HOST", "OLLAMA_API_KEY", "ADMIN_EMAIL"]
    faltan = [n for n in obligatorios if not prov.get(n) or prov[n].startswith("<")]
    # auth valida el email (pydantic EmailStr): rechaza los dominios reservados y el admin no podría entrar.
    local, arroba, dominio = prov.get("ADMIN_EMAIL", "").lower().rpartition("@")
    valido = bool(local and arroba and "." in dominio and dominio.rsplit(".", 1)[-1] not in DOMINIOS_RESERVADOS)
    if "ADMIN_EMAIL" not in faltan and not valido:
        faltan.append("ADMIN_EMAIL (con un dominio real: auth rechaza .local, .test, .localhost, .invalid y .example)")
    return faltan


def variables_por_servicio(prov: dict[str, str], sec: dict[str, str]) -> dict[str, dict[str, str]]:
    """Valor de cada variable `sync: false`, por nombre de servicio del render.yaml."""
    mecanismo = prov["AIVEN_SASL_MECANISMO"].upper()
    if mecanismo not in ("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"):
        raise SystemExit(f"AIVEN_SASL_MECANISMO no soportado: {mecanismo}")
    modulo = ("org.apache.kafka.common.security.plain.PlainLoginModule" if mecanismo == "PLAIN"
              else "org.apache.kafka.common.security.scram.ScramLoginModule")
    jaas = f'{modulo} required username="{prov["AIVEN_USUARIO"]}" password="{prov["AIVEN_CLAVE"]}";'
    host, ref = prov["SUPABASE_POOLER_HOST"], prov["SUPABASE_REF"]

    def usuario(rol: str) -> str:
        return f"{rol}.{ref}"  # el session pooler de Supabase identifica el proyecto por el usuario

    def url_pg(rol: str, driver: str = "postgresql") -> str:
        return f"{driver}://{usuario(rol)}:{sec[ROLES_DB[rol]]}@{host}:5432/postgres?sslmode=require"

    jdbc = f"jdbc:postgresql://{host}:5432/postgres?sslmode=require"
    kafka_python = {
        "KAFKA_BOOTSTRAP_SERVERS": prov["AIVEN_BOOTSTRAP"],
        "KAFKA_SASL_MECHANISM": mecanismo,
        "KAFKA_SASL_USERNAME": prov["AIVEN_USUARIO"],
        "KAFKA_SASL_PASSWORD": prov["AIVEN_CLAVE"],
    }
    kafka_spring = {
        "SPRING_KAFKA_PROPERTIES_SASL_MECHANISM": mecanismo,
        "SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG": jaas,
    }
    claves_servicio = (f"leads={sec['CLAVE_ORQ_LEADS']},calificacion={sec['CLAVE_ORQ_CALIFICACION']},"
                       f"ia-resumen={sec['CLAVE_ORQ_IA_RESUMEN']}")
    return {
        "dda2-g8-orquestador": {
            "JWT_SECRET": sec["JWT_SECRET"],
            "CLAVES_SERVICIO": claves_servicio,
            "VEHICULOS_API_KEY": sec["VEHICULOS_CLAVE_ESCRITURA"],
            "DATABASE_URL": url_pg("orquestador"),
            **kafka_python,
        },
        "dda2-g8-auth": {
            # psycopg 3: SQLAlchemy necesita el driver explícito.
            "DATABASE_URL": url_pg("crm_auth", "postgresql+psycopg"),
            "JWT_SECRET": sec["JWT_SECRET"],
            "SEED_ADMIN_EMAIL": prov["ADMIN_EMAIL"],
            "SEED_ADMIN_PASSWORD": sec["ADMIN_PASSWORD"],
        },
        "dda2-g8-leads": {
            "DATABASE_URL": url_pg("leads", "postgresql+psycopg"),
            "CLIENTES_CLAVE_LEADS": sec["CLIENTES_CLAVE_LEADS"],
            "ORQUESTADOR_CLAVE_SERVICIO": sec["CLAVE_ORQ_LEADS"],
            **kafka_python,
        },
        "dda2-g8-calificacion": {
            "SPRING_DATASOURCE_URL": jdbc,
            "SPRING_DATASOURCE_USERNAME": usuario("calificacion"),
            "SPRING_DATASOURCE_PASSWORD": sec["CLAVE_DB_CALIFICACION"],
            "CLIENTES_CLAVE_CALIFICACION": sec["CLIENTES_CLAVE_CALIFICACION"],
            "ORQUESTADOR_CLAVE_SERVICIO": sec["CLAVE_ORQ_CALIFICACION"],
            "SPRING_KAFKA_BOOTSTRAP_SERVERS": prov["AIVEN_BOOTSTRAP"],
            **kafka_spring,
        },
        "dda2-g8-clientes": {
            "CLIENTES_DB_URL": jdbc,
            "CLIENTES_DB_USUARIO": usuario("clientes"),
            "CLIENTES_DB_CLAVE": sec["CLAVE_DB_CLIENTES"],
            "CLIENTES_CUIL_CLAVE": sec["CLIENTES_CUIL_CLAVE"],
            "CLIENTES_CUIL_HMAC": sec["CLIENTES_CUIL_HMAC"],
            "CLIENTES_CLAVE_LEADS": sec["CLIENTES_CLAVE_LEADS"],
            "CLIENTES_CLAVE_CALIFICACION": sec["CLIENTES_CLAVE_CALIFICACION"],
            "KAFKA_BOOTSTRAP_SERVERS": prov["AIVEN_BOOTSTRAP"],
            **kafka_spring,
        },
        "dda2-g8-vehiculos": {
            "VEHICULOS_DB_URL": jdbc,
            "VEHICULOS_DB_USUARIO": usuario("vehiculos"),
            "VEHICULOS_DB_CLAVE": sec["CLAVE_DB_VEHICULOS"],
            "VEHICULOS_CLAVE_ESCRITURA": sec["VEHICULOS_CLAVE_ESCRITURA"],
        },
        "dda2-g8-ia-resumen": {
            "OLLAMA_API_KEY": prov["OLLAMA_API_KEY"],
            "ORQUESTADOR_CLAVE_SERVICIO": sec["CLAVE_ORQ_IA_RESUMEN"],
            **kafka_python,
        },
        "dda2-g8-multas": dict(kafka_python),
    }


def secretos_del_blueprint(ruta: Path = RAIZ / "render.yaml") -> dict[str, list[str]]:
    """Las variables `sync: false` de cada servicio del render.yaml."""
    blueprint = yaml.safe_load(ruta.read_text(encoding="utf-8"))
    return {
        servicio["name"]: [v["key"] for v in servicio.get("envVars", []) if v.get("sync") is False]
        for servicio in blueprint["services"]
    }


def verificar(variables: dict[str, dict[str, str]], esperadas: dict[str, list[str]]) -> list[str]:
    """Diferencias entre lo que pide el render.yaml y lo que arma este script (vacío = coinciden)."""
    problemas = []
    for servicio, claves in esperadas.items():
        tiene = variables.get(servicio, {})
        for clave in claves:
            if not tiene.get(clave):
                problemas.append(f"{servicio}: falta {clave}")
        for clave in tiene:
            if clave not in claves:
                problemas.append(f"{servicio}: {clave} no es sync: false en render.yaml")
    for servicio in variables:
        if servicio not in esperadas:
            problemas.append(f"{servicio}: no existe en render.yaml")
    return problemas


def escribir_variables(variables: dict[str, dict[str, str]]) -> Path:
    ruta = SALIDA / "variables-por-servicio.md"
    partes = ["# Variables `sync: false` por servicio (NO subir a git)\n",
              "En Render, al crear el Blueprint, pegar cada bloque en su servicio (o después: Environment > "
              "Add from .env).\n"]
    for servicio, valores in variables.items():
        cuerpo = "\n".join(f"{clave}={valor}" for clave, valor in valores.items())
        partes.append(f"\n## {servicio}\n\n```env\n{cuerpo}\n```\n")
    ruta.write_text("".join(partes), encoding="utf-8")
    return ruta


def escribir_sql_roles(sec: dict[str, str]) -> Path:
    plantilla = (RAIZ / "infra/supabase/02-crear-roles-y-schemas.sql").read_text(encoding="utf-8")
    for clave in ROLES_DB.values():
        plantilla = plantilla.replace("{{" + clave + "}}", sec[clave])
    if "'{{CLAVE" in plantilla:
        raise SystemExit("Quedó una clave sin reemplazar en el SQL de roles.")
    ruta = SALIDA / "supabase-roles.sql"
    ruta.write_text(plantilla, encoding="utf-8")
    return ruta


def _ruta(ruta: Path) -> str:
    """La ruta relativa al repo si está adentro (para mostrarla)."""
    try:
        return str(ruta.relative_to(RAIZ))
    except ValueError:
        return str(ruta)


def main() -> int:
    SALIDA.mkdir(exist_ok=True)
    if not PROVEEDORES.exists():
        PROVEEDORES.write_text(PLANTILLA_PROVEEDORES, encoding="utf-8")
        print(f"Completá {_ruta(PROVEEDORES)} y volvé a correr el script.")
        return 2
    proveedores = leer_env(PROVEEDORES)
    faltan = _faltantes_proveedores(proveedores)
    if faltan:
        print(f"Faltan datos en {_ruta(PROVEEDORES)}: {', '.join(faltan)}")
        return 2
    secretos_generados = asegurar_secretos()
    variables = variables_por_servicio(proveedores, secretos_generados)
    problemas = verificar(variables, secretos_del_blueprint())
    if problemas:
        print("El render.yaml y este script no coinciden:\n  " + "\n  ".join(problemas))
        return 1
    print(f"Listo: {_ruta(escribir_variables(variables))} y {_ruta(escribir_sql_roles(secretos_generados))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
