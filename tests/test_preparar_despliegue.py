"""scripts/preparar_despliegue.py: secretos del despliegue y variables por servicio del render.yaml."""

from __future__ import annotations

import base64
import importlib.util
from pathlib import Path

import pytest
import yaml

RAIZ = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("preparar_despliegue", RAIZ / "scripts" / "preparar_despliegue.py")
pd = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pd)

PROVEEDORES = {
    "AIVEN_BOOTSTRAP": "kafka-crm.aivencloud.com:12345",
    "AIVEN_SASL_MECANISMO": "SCRAM-SHA-256",
    "AIVEN_USUARIO": "avnadmin",
    "AIVEN_CLAVE": "clave-aiven",
    "SUPABASE_REF": "abcdefghij",
    "SUPABASE_POOLER_HOST": "aws-0-sa-east-1.pooler.supabase.com",
    "OLLAMA_API_KEY": "clave-ollama",
    "ADMIN_EMAIL": "admin@dda2-crm.com",
}


@pytest.fixture
def salida(tmp_path, monkeypatch):
    monkeypatch.setattr(pd, "SALIDA", tmp_path)
    monkeypatch.setattr(pd, "PROVEEDORES", tmp_path / "proveedores.env")
    monkeypatch.setattr(pd, "SECRETOS", tmp_path / "secretos.env")
    return tmp_path


def _completar_proveedores(salida: Path) -> None:
    (salida / "proveedores.env").write_text("\n".join(f"{k}={v}" for k, v in PROVEEDORES.items()), encoding="utf-8")


def test_el_render_yaml_es_un_blueprint_valido_con_todos_los_servicios() -> None:
    blueprint = yaml.safe_load((RAIZ / "render.yaml").read_text(encoding="utf-8"))
    nombres = [s["name"] for s in blueprint["services"]]

    assert sorted(nombres) == sorted([
        "dda2-g8-orquestador", "dda2-g8-auth", "dda2-g8-leads", "dda2-g8-calificacion", "dda2-g8-clientes",
        "dda2-g8-vehiculos", "dda2-g8-ia-resumen", "dda2-g8-multas", "dda2-g8-monitor", "dda2-g8-front",
    ])
    for servicio in blueprint["services"]:
        assert servicio["repo"].startswith("https://github.com/"), servicio["name"]
        assert servicio["branch"] in ("main", "dev"), servicio["name"]
        claves = [v["key"] for v in servicio.get("envVars", [])]
        assert len(claves) == len(set(claves)), f"variable repetida en {servicio['name']}"
        if servicio["runtime"] != "static":
            assert servicio["plan"] == "free"


def test_las_urls_entre_servicios_apuntan_a_servicios_del_blueprint() -> None:
    blueprint = yaml.safe_load((RAIZ / "render.yaml").read_text(encoding="utf-8"))
    nombres = {s["name"] for s in blueprint["services"]}

    for servicio in blueprint["services"]:
        for variable in servicio.get("envVars", []):
            valor = str(variable.get("value", ""))
            for url in [p for p in valor.split(",") if ".onrender.com" in p]:
                host = url.split("//", 1)[1].split(".onrender.com", 1)[0]
                assert host in nombres, f"{servicio['name']}.{variable['key']} apunta a {host}, que no existe"


def test_los_servicios_que_llaman_por_el_gateway_usan_la_url_del_orquestador_con_api() -> None:
    blueprint = {s["name"]: s for s in yaml.safe_load((RAIZ / "render.yaml").read_text(encoding="utf-8"))["services"]}

    def valor(servicio: str, clave: str) -> str:
        return next(v["value"] for v in blueprint[servicio]["envVars"] if v["key"] == clave)

    gateway = "https://dda2-g8-orquestador.onrender.com/api"
    assert valor("dda2-g8-leads", "CLIENTES_URL") == gateway
    assert valor("dda2-g8-calificacion", "CLIENTES_SERVICE_URL") == gateway
    assert valor("dda2-g8-calificacion", "VEHICULOS_SERVICE_URL") == gateway
    assert valor("dda2-g8-ia-resumen", "CLIENTES_URL") == gateway
    assert valor("dda2-g8-ia-resumen", "VEHICULOS_URL") == gateway
    assert valor("dda2-g8-front", "VITE_API_URL") == gateway
    # auth no puede usar el schema `auth`: en Supabase es el de su propio servicio de autenticación
    assert valor("dda2-g8-auth", "DB_SCHEMA") == "crm_auth"


def test_la_primera_vez_pide_completar_los_proveedores(salida: Path) -> None:
    assert pd.main() == 2
    assert (salida / "proveedores.env").exists()
    assert not (salida / "secretos.env").exists()


def test_con_proveedores_incompletos_no_genera_nada(salida: Path) -> None:
    pd.main()  # crea la plantilla, con valores de ejemplo

    assert pd.main() == 2
    assert not (salida / "variables-por-servicio.md").exists()


def test_cubre_todas_las_variables_sync_false_del_render_yaml(salida: Path) -> None:
    _completar_proveedores(salida)

    assert pd.main() == 0

    variables = pd.variables_por_servicio(PROVEEDORES, pd.leer_env(salida / "secretos.env"))
    assert pd.verificar(variables, pd.secretos_del_blueprint()) == []
    texto = (salida / "variables-por-servicio.md").read_text(encoding="utf-8")
    for servicio, claves in pd.secretos_del_blueprint().items():
        for clave in claves:
            assert f"{clave}=" in texto, (servicio, clave)


def test_verificar_detecta_una_variable_que_falta_o_sobra() -> None:
    esperadas = {"svc": ["A", "B"]}

    assert pd.verificar({"svc": {"A": "1"}}, esperadas) == ["svc: falta B"]
    assert pd.verificar({"svc": {"A": "1", "B": "2", "C": "3"}}, esperadas) == ["svc: C no es sync: false en render.yaml"]
    assert pd.verificar({"svc": {"A": "1", "B": "2"}, "otro": {}}, esperadas) == ["otro: no existe en render.yaml"]


def test_los_secretos_se_generan_una_vez_y_no_se_regeneran(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    primera = pd.leer_env(salida / "secretos.env")

    pd.main()

    assert pd.leer_env(salida / "secretos.env") == primera


def test_un_secreto_que_falta_se_agrega_sin_tocar_los_demas(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    antes = pd.leer_env(salida / "secretos.env")
    lineas = [l for l in (salida / "secretos.env").read_text(encoding="utf-8").splitlines() if not l.startswith("JWT_SECRET=")]
    (salida / "secretos.env").write_text("\n".join(lineas) + "\n", encoding="utf-8")

    pd.main()

    despues = pd.leer_env(salida / "secretos.env")
    assert despues["JWT_SECRET"] and despues["JWT_SECRET"] != antes["JWT_SECRET"]
    assert {k: v for k, v in despues.items() if k != "JWT_SECRET"} == {k: v for k, v in antes.items() if k != "JWT_SECRET"}


def test_las_claves_son_largas_y_distintas_y_las_del_cuil_son_32_bytes_en_base64(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    sec = pd.leer_env(salida / "secretos.env")

    for nombre, valor in sec.items():
        assert len(valor) >= 32, nombre
    assert len(set(sec.values())) == len(sec)
    for nombre in ("CLIENTES_CUIL_CLAVE", "CLIENTES_CUIL_HMAC"):
        assert len(base64.b64decode(sec[nombre])) == 32


def test_las_credenciales_compartidas_coinciden_entre_servicios(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    v = pd.variables_por_servicio(PROVEEDORES, pd.leer_env(salida / "secretos.env"))

    assert v["dda2-g8-orquestador"]["JWT_SECRET"] == v["dda2-g8-auth"]["JWT_SECRET"]
    assert v["dda2-g8-orquestador"]["VEHICULOS_API_KEY"] == v["dda2-g8-vehiculos"]["VEHICULOS_CLAVE_ESCRITURA"]
    assert v["dda2-g8-leads"]["CLIENTES_CLAVE_LEADS"] == v["dda2-g8-clientes"]["CLIENTES_CLAVE_LEADS"]
    assert (v["dda2-g8-calificacion"]["CLIENTES_CLAVE_CALIFICACION"]
            == v["dda2-g8-clientes"]["CLIENTES_CLAVE_CALIFICACION"])
    claves = dict(p.split("=", 1) for p in v["dda2-g8-orquestador"]["CLAVES_SERVICIO"].split(","))
    assert claves == {
        "leads": v["dda2-g8-leads"]["ORQUESTADOR_CLAVE_SERVICIO"],
        "calificacion": v["dda2-g8-calificacion"]["ORQUESTADOR_CLAVE_SERVICIO"],
        "ia-resumen": v["dda2-g8-ia-resumen"]["ORQUESTADOR_CLAVE_SERVICIO"],
    }


def test_las_conexiones_a_supabase_usan_el_pooler_con_el_usuario_rol_punto_ref(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    sec = pd.leer_env(salida / "secretos.env")
    v = pd.variables_por_servicio(PROVEEDORES, sec)

    host = "aws-0-sa-east-1.pooler.supabase.com:5432/postgres?sslmode=require"
    assert v["dda2-g8-orquestador"]["DATABASE_URL"] == (
        f"postgresql://orquestador.abcdefghij:{sec['CLAVE_DB_ORQUESTADOR']}@{host}")
    # leads y auth usan psycopg 3: el driver va explícito
    assert v["dda2-g8-leads"]["DATABASE_URL"].startswith("postgresql+psycopg://leads.abcdefghij:")
    assert v["dda2-g8-auth"]["DATABASE_URL"].startswith("postgresql+psycopg://crm_auth.abcdefghij:")
    assert v["dda2-g8-vehiculos"]["VEHICULOS_DB_URL"] == f"jdbc:postgresql://{host}"
    assert v["dda2-g8-vehiculos"]["VEHICULOS_DB_USUARIO"] == "vehiculos.abcdefghij"
    assert v["dda2-g8-clientes"]["CLIENTES_DB_USUARIO"] == "clientes.abcdefghij"
    assert v["dda2-g8-calificacion"]["SPRING_DATASOURCE_USERNAME"] == "calificacion.abcdefghij"


@pytest.mark.parametrize(
    "mecanismo, modulo",
    [
        ("SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule"),
        ("scram-sha-512", "org.apache.kafka.common.security.scram.ScramLoginModule"),
        ("PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule"),
    ],
)
def test_el_jaas_de_los_servicios_java_sigue_el_mecanismo_de_aiven(mecanismo: str, modulo: str) -> None:
    sec = {nombre: "x" * 40 for nombre in pd.GENERADORES}
    v = pd.variables_por_servicio({**PROVEEDORES, "AIVEN_SASL_MECANISMO": mecanismo}, sec)

    jaas = v["dda2-g8-clientes"]["SPRING_KAFKA_PROPERTIES_SASL_JAAS_CONFIG"]
    assert jaas == f'{modulo} required username="avnadmin" password="clave-aiven";'
    assert v["dda2-g8-calificacion"]["SPRING_KAFKA_PROPERTIES_SASL_MECHANISM"] == mecanismo.upper()
    assert v["dda2-g8-multas"]["KAFKA_SASL_MECHANISM"] == mecanismo.upper()


def test_un_mecanismo_sasl_desconocido_se_rechaza() -> None:
    with pytest.raises(SystemExit):
        pd.variables_por_servicio({**PROVEEDORES, "AIVEN_SASL_MECANISMO": "GSSAPI"}, {n: "x" for n in pd.GENERADORES})


def test_el_sql_de_roles_lleva_las_claves_reales_y_ningun_marcador(salida: Path) -> None:
    _completar_proveedores(salida)
    pd.main()
    sec = pd.leer_env(salida / "secretos.env")

    sql = (salida / "supabase-roles.sql").read_text(encoding="utf-8")

    assert "'{{CLAVE" not in sql
    for rol, clave in pd.ROLES_DB.items():
        assert f"('{rol}'," in sql and sec[clave] in sql


@pytest.mark.parametrize("email", ["admin@dda2.local", "admin@crm.test", "admin@localhost", "sin-arroba"])
def test_un_email_de_admin_con_dominio_reservado_se_rechaza(salida: Path, email: str) -> None:
    _completar_proveedores(salida)
    texto = (salida / "proveedores.env").read_text(encoding="utf-8").replace("admin@dda2-crm.com", email)
    (salida / "proveedores.env").write_text(texto, encoding="utf-8")

    assert pd.main() == 2
    assert not (salida / "secretos.env").exists()
