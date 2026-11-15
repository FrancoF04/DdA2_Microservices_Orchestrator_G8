"""Autenticación del gateway: rutas públicas y protegidas, JWT, servicios, identidad y logout."""

from __future__ import annotations

import time
import uuid
from collections.abc import Iterator
from typing import Optional

import httpx
import jwt
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import crear_app
from tests.test_proxy import AuditoriaFalsa, ServiciosFalsos

SECRETO = "s" * 40
CLAVE_LEADS = "clave-de-leads-de-prueba-con-32-caracteres"
CLAVE_CALIFICACION = "clave-de-calificacion-de-prueba-32-caracteres"
CLAVE_IA = "clave-de-ia-resumen-de-prueba-con-32-caracteres"
ORIGEN_FRONT = "http://front.test"


def crear_token(secreto: str = SECRETO, algoritmo: str = "HS256", **cambios: Optional[object]) -> str:
    ahora = int(time.time())
    claims: dict[str, object] = {
        "sub": "7d0f6b52-0000-4000-8000-000000000001",
        "email": "ana@crm.test",
        "rol": "TELEMARKETING",
        "jti": str(uuid.uuid4()),
        "iat": ahora,
        "exp": ahora + 900,
    }
    claims.update(cambios)
    claims = {k: v for k, v in claims.items() if v is not None}  # None = sacar el claim
    return jwt.encode(claims, secreto, algorithm=algoritmo)


def bearer(token: Optional[str] = None) -> dict[str, str]:
    return {"Authorization": f"Bearer {token or crear_token()}"}


def de_servicio(nombre: str, clave: str) -> dict[str, str]:
    return {"X-Origen-Servicio": nombre, "X-Clave-Servicio": clave}


@pytest.fixture
def servicios() -> ServiciosFalsos:
    return ServiciosFalsos()


@pytest.fixture
def auditoria() -> AuditoriaFalsa:
    return AuditoriaFalsa()


@pytest.fixture
def gw(servicios: ServiciosFalsos, auditoria: AuditoriaFalsa) -> Iterator[TestClient]:
    settings = Settings(
        _env_file=None,
        kafka_enabled=False,
        database_url=None,
        cors_allowed_origins=ORIGEN_FRONT,
        leads_url="http://leads.test",
        clientes_url="http://clientes.test",
        vehiculos_url="http://vehiculos.test",
        calificacion_url="http://calificacion.test",
        auth_url="http://auth.test",
        auth_habilitada=True,
        jwt_secret=SECRETO,
        claves_servicio=f"leads={CLAVE_LEADS},calificacion={CLAVE_CALIFICACION},ia-resumen={CLAVE_IA}",
    )
    app = crear_app(settings, transporte_http=httpx.MockTransport(servicios))
    app.state.auditoria = auditoria
    with TestClient(app) as client:
        yield client


# --------------------------------------------------------------- rutas públicas


def test_el_preformulario_no_pide_login(gw, servicios):
    r = gw.post("/api/leads", json={"cliente": {}})
    assert r.status_code == 200
    assert servicios.recibidas[-1].url.path == "/leads"


def test_el_catalogo_de_vehiculos_es_publico(gw, servicios):
    assert gw.get("/api/vehiculos").status_code == 200
    assert gw.get("/api/vehiculos?disponible=true").status_code == 200
    assert gw.get("/api/vehiculos/a0000000-0000-4000-8000-000000000001").status_code == 200
    assert gw.get("/api/vehiculos/a0000000-0000-4000-8000-000000000001/foto").status_code == 200
    assert len(servicios.recibidas) == 4


def test_las_escrituras_de_vehiculos_con_su_clave_pasan_y_las_validan_ellos(gw, servicios):
    r = gw.post("/api/vehiculos", json={}, headers={"X-Api-Key": "clave-de-escritura"})
    assert r.status_code == 200
    # La clave sigue de largo: la valida vehiculos, no el orquestador.
    assert servicios.recibidas[-1].headers["x-api-key"] == "clave-de-escritura"


def test_las_escrituras_de_vehiculos_sin_clave_ni_login_se_rechazan(gw, servicios):
    assert gw.post("/api/vehiculos", json={}).status_code == 401
    assert gw.put("/api/vehiculos/1", json={}).status_code == 401
    assert gw.delete("/api/vehiculos/1").status_code == 401
    assert servicios.recibidas == []


@pytest.mark.parametrize("ruta", ["/api/auth/login", "/api/auth/refresh", "/api/auth/logout"])
def test_login_refresh_y_logout_no_piden_token(gw, servicios, ruta):
    assert gw.post(ruta, json={}).status_code == 200
    assert servicios.recibidas[-1].url.path == ruta.removeprefix("/api")


def test_el_429_del_login_llega_al_front_con_su_retry_after_legible(gw, servicios):
    """Auth limita los intentos de login: el front tiene que poder leer cuánto falta."""
    servicios.responder = lambda r: httpx.Response(
        429, json={"detail": "Demasiados intentos fallidos."}, headers={"Retry-After": "300"}
    )
    r = gw.post("/api/auth/login", json={}, headers={"Origin": ORIGEN_FRONT})
    assert r.status_code == 429
    assert r.headers["retry-after"] == "300"
    assert "retry-after" in r.headers["access-control-expose-headers"].lower()


def test_el_preflight_de_cors_no_pide_token(gw):
    r = gw.options(
        "/api/leads",
        headers={
            "Origin": ORIGEN_FRONT,
            "Access-Control-Request-Method": "GET",
            "Access-Control-Request-Headers": "authorization,content-type",
        },
    )
    assert r.status_code == 200
    assert "authorization" in r.headers["access-control-allow-headers"].lower()


def test_health_y_docs_no_piden_token(gw):
    assert gw.get("/health").status_code == 200
    assert gw.get("/openapi.json").status_code == 200


# ----------------------------------------------------------- rutas protegidas


@pytest.mark.parametrize(
    ("metodo", "ruta"),
    [
        ("GET", "/api/leads"),
        ("GET", "/api/leads/123"),
        ("PATCH", "/api/leads/123/state"),
        ("POST", "/api/leads/123"),  # solo POST /api/leads es público
        ("GET", "/api/calificaciones/123"),
        ("POST", "/api/calificaciones/123/correcciones"),
        ("GET", "/api/clientes/123"),
        ("GET", "/api/clientes?q=ana"),
        ("GET", "/api/clientes/123/historial"),
        ("PATCH", "/api/clientes/123"),
        ("GET", "/api/monitor/estado"),
        ("POST", "/api/resumen"),
        ("GET", "/api/auth/me"),
        ("GET", "/api/auth/usuarios"),
        ("GET", "/api/multas/ABC123"),
        ("GET", "/api/orquestador/servicios"),
        ("GET", "/api/orquestador/flujos"),
    ],
)
def test_las_rutas_protegidas_piden_login(gw, servicios, metodo, ruta):
    r = gw.request(metodo, ruta)
    assert r.status_code == 401, ruta
    cuerpo = r.json()
    assert cuerpo["codigo"] == "NO_AUTENTICADO"
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.headers["www-authenticate"] == "Bearer"
    assert servicios.recibidas == []  # ni siquiera llegó al servicio


def test_con_token_valido_pasa_y_el_servicio_recibe_la_identidad(gw, servicios):
    r = gw.get("/api/leads", headers=bearer())
    assert r.status_code == 200
    recibida = servicios.recibidas[-1]
    assert recibida.headers["x-usuario"] == "ana@crm.test"
    assert recibida.headers["x-usuario-rol"] == "TELEMARKETING"


def test_el_jwt_no_se_reenvia_a_los_servicios_que_no_lo_necesitan(gw, servicios):
    gw.get("/api/leads", headers=bearer())
    assert "authorization" not in servicios.recibidas[-1].headers


def test_auth_si_recibe_el_jwt(gw, servicios):
    token = crear_token()
    gw.get("/api/auth/me", headers=bearer(token))
    assert servicios.recibidas[-1].headers["authorization"] == f"Bearer {token}"


def test_el_token_sin_email_usa_el_id_como_usuario(gw, servicios):
    gw.get("/api/leads", headers=bearer(crear_token(email=None)))
    assert servicios.recibidas[-1].headers["x-usuario"] == "7d0f6b52-0000-4000-8000-000000000001"


def test_un_email_con_caracteres_raros_no_se_inyecta_en_el_header(gw, servicios):
    gw.get("/api/leads", headers=bearer(crear_token(email="ana\r\nX-Admin: 1@crm.test")))
    recibida = servicios.recibidas[-1]
    assert recibida.headers["x-usuario"] == "7d0f6b52-0000-4000-8000-000000000001"
    assert "x-admin" not in recibida.headers


# ----------------------------------------------------- identidad y su falsificación


def test_un_x_usuario_falsificado_se_reemplaza_por_el_del_token(gw, servicios):
    gw.get("/api/leads", headers={**bearer(), "X-Usuario": "root", "X-Usuario-Rol": "ADMIN"})
    recibida = servicios.recibidas[-1]
    assert recibida.headers.get_list("x-usuario") == ["ana@crm.test"]
    assert recibida.headers.get_list("x-usuario-rol") == ["TELEMARKETING"]


def test_un_x_usuario_en_una_ruta_publica_se_descarta(gw, servicios):
    gw.post("/api/leads", json={}, headers={"X-Usuario": "root", "X-Usuario-Rol": "ADMIN"})
    recibida = servicios.recibidas[-1]
    assert "x-usuario" not in recibida.headers
    assert "x-usuario-rol" not in recibida.headers


def test_un_servicio_autenticado_no_puede_hacerse_pasar_por_un_operador(gw, servicios):
    gw.get("/api/clientes/1", headers={**de_servicio("ia-resumen", CLAVE_IA), "X-Usuario": "root"})
    assert "x-usuario" not in servicios.recibidas[-1].headers


# ------------------------------------------------------------ tokens inválidos


def test_token_vencido(gw):
    r = gw.get("/api/leads", headers=bearer(crear_token(exp=int(time.time()) - 600)))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_EXPIRADO"


def test_token_firmado_con_otro_secreto(gw):
    r = gw.get("/api/leads", headers=bearer(crear_token(secreto="otro" * 12)))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_INVALIDO"


def test_token_con_algoritmo_none(gw):
    ahora = int(time.time())
    sin_firma = jwt.encode(
        {"sub": "u", "jti": "j", "exp": ahora + 900}, key=None, algorithm="none"
    )
    r = gw.get("/api/leads", headers=bearer(sin_firma))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_INVALIDO"


def test_token_con_otro_algoritmo_hmac_se_rechaza(gw):
    r = gw.get("/api/leads", headers=bearer(crear_token(algoritmo="HS512")))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_INVALIDO"


@pytest.mark.parametrize("claim", ["jti", "sub", "exp"])
def test_token_sin_un_claim_obligatorio(gw, claim):
    r = gw.get("/api/leads", headers=bearer(crear_token(**{claim: None})))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_INVALIDO"


@pytest.mark.parametrize("valor", ["Bearer", "Bearer ", "Basic abc", "abc.def.ghi", "Bearer no-es-un-jwt"])
def test_authorization_mal_formado(gw, valor):
    r = gw.get("/api/leads", headers={"Authorization": valor})
    assert r.status_code == 401


def test_un_recurso_inexistente_no_se_revela_sin_login(gw):
    assert gw.get("/api/inexistente").status_code == 401
    assert gw.get("/api/inexistente", headers=bearer()).status_code == 404


def test_los_endpoints_propios_piden_login_y_aceptan_un_operador(gw):
    assert gw.get("/api/orquestador/flujos").status_code == 401
    # Sin base devuelve 503, pero ya no 401: el token alcanzó.
    assert gw.get("/api/orquestador/flujos", headers=bearer()).status_code != 401


def test_un_servicio_no_puede_usar_los_endpoints_propios(gw):
    r = gw.get("/api/orquestador/servicios", headers=de_servicio("leads", CLAVE_LEADS))
    assert r.status_code == 401


# ------------------------------------------------------- credencial de servicio


def test_ia_resumen_con_su_clave_pide_el_cliente(gw, servicios, auditoria):
    r = gw.get("/api/clientes/123", headers=de_servicio("ia-resumen", CLAVE_IA))
    assert r.status_code == 200
    recibida = servicios.recibidas[-1]
    assert recibida.url.path == "/clientes/123"
    assert "x-clave-servicio" not in recibida.headers  # el secreto no sigue de largo
    assert auditoria.llamadas[-1].origen == "ia-resumen"  # origen verificado, no declarado


def test_leads_da_de_alta_el_cliente_con_su_clave_y_la_de_clientes_sigue_de_largo(gw, servicios):
    headers = {**de_servicio("leads", CLAVE_LEADS), "X-Api-Key": "clave-que-valida-clientes"}
    assert gw.post("/api/clientes", json={}, headers=headers).status_code == 200
    assert servicios.recibidas[-1].headers["x-api-key"] == "clave-que-valida-clientes"


def test_calificacion_consulta_clientes(gw):
    r = gw.get("/api/clientes/1/situacion-crediticia", headers=de_servicio("calificacion", CLAVE_CALIFICACION))
    assert r.status_code == 200


@pytest.mark.parametrize(
    "headers",
    [
        de_servicio("ia-resumen", "clave-equivocada-" + "x" * 30),
        de_servicio("ia-resumen", CLAVE_LEADS),  # la de otro servicio
        de_servicio("servicio-inventado", CLAVE_IA),
        de_servicio("", CLAVE_IA),
        {"X-Clave-Servicio": CLAVE_IA},  # sin decir quién es
    ],
)
def test_credencial_de_servicio_invalida(gw, servicios, headers):
    r = gw.get("/api/clientes/1", headers=headers)
    assert r.status_code == 401
    assert r.json()["codigo"] == "CLAVE_SERVICIO_INVALIDA"
    assert servicios.recibidas == []


def test_un_servicio_no_llama_a_lo_que_no_le_corresponde(gw, servicios):
    r = gw.get("/api/calificaciones/1", headers=de_servicio("leads", CLAVE_LEADS))
    assert r.status_code == 403
    assert r.json()["codigo"] == "SERVICIO_NO_AUTORIZADO"
    assert servicios.recibidas == []


def test_declarar_un_origen_sin_clave_no_alcanza(gw):
    r = gw.get("/api/clientes/1", headers={"X-Origen-Servicio": "ia-resumen"})
    assert r.status_code == 401
    assert r.json()["codigo"] == "NO_AUTENTICADO"


# ----------------------------------------------------------------------- logout


def test_el_logout_invalida_el_token_en_el_acto(gw, servicios):
    token = crear_token()
    otro = crear_token()
    assert gw.get("/api/leads", headers=bearer(token)).status_code == 200

    r = gw.post("/api/auth/logout", json={"refresh_token": "x"}, headers=bearer(token))
    assert r.status_code == 200
    assert servicios.recibidas[-1].url.path == "/auth/logout"  # auth también se entera

    r = gw.get("/api/leads", headers=bearer(token))
    assert r.status_code == 401
    assert r.json()["codigo"] == "TOKEN_REVOCADO"
    # Los tokens de otras sesiones siguen sirviendo.
    assert gw.get("/api/leads", headers=bearer(otro)).status_code == 200


def test_el_logout_con_un_token_vencido_o_ajeno_no_revoca_nada(gw):
    token = crear_token()
    gw.post("/api/auth/logout", json={}, headers=bearer(crear_token(secreto="otro" * 12)))
    gw.post("/api/auth/logout", json={})
    assert gw.get("/api/leads", headers=bearer(token)).status_code == 200


def test_el_logout_se_aplica_aunque_auth_este_caido(gw, servicios):
    token = crear_token()

    def caido(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("auth caído")

    servicios.responder = caido
    assert gw.post("/api/auth/logout", json={}, headers=bearer(token)).status_code == 503
    servicios.responder = lambda r: httpx.Response(200, json={"ok": True})
    assert gw.get("/api/leads", headers=bearer(token)).status_code == 401


# ----------------------------------------------------------- autenticación apagada


def test_con_la_autenticacion_apagada_todo_pasa(servicios, auditoria):
    settings = Settings(
        _env_file=None, kafka_enabled=False, database_url=None, auth_habilitada=False, leads_url="http://leads.test"
    )
    app = crear_app(settings, transporte_http=httpx.MockTransport(servicios))
    app.state.auditoria = auditoria
    with TestClient(app) as c:
        assert c.get("/api/leads").status_code == 200
