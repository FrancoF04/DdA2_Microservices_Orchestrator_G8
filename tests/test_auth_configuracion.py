"""Configuración de la autenticación: el orquestador no arranca abierto ni mal configurado."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import crear_app
from app.seguridad.autenticador import validar_configuracion_auth
from app.seguridad.politica import es_publica
from app.seguridad.revocaciones import Revocaciones

SECRETO = "s" * 40
CLAVES = "leads=" + "a" * 32 + ",calificacion=" + "b" * 32 + ",ia-resumen=" + "c" * 32


def settings(**cambios) -> Settings:
    base = dict(_env_file=None, auth_habilitada=True, jwt_secret=SECRETO, claves_servicio=CLAVES)
    base.update(cambios)
    return Settings(**base)


def test_una_configuracion_completa_es_valida():
    validar_configuracion_auth(settings())


def test_apagada_no_valida_nada():
    validar_configuracion_auth(settings(auth_habilitada=False, jwt_secret=None))


def test_no_arranca_sin_secreto():
    with pytest.raises(RuntimeError, match="JWT_SECRET"):
        validar_configuracion_auth(settings(jwt_secret=None))


def test_un_secreto_vacio_cuenta_como_ausente():
    # En un .env, `JWT_SECRET=` llega como string vacío.
    with pytest.raises(RuntimeError, match="JWT_SECRET"):
        validar_configuracion_auth(settings(jwt_secret="  "))


def test_no_arranca_con_secreto_corto():
    with pytest.raises(RuntimeError, match="corto"):
        validar_configuracion_auth(settings(jwt_secret="corto"))


@pytest.mark.parametrize("algoritmo", ["none", "RS256", "ES256", ""])
def test_no_arranca_con_un_algoritmo_no_permitido(algoritmo):
    with pytest.raises(RuntimeError, match="JWT_ALGORITHM"):
        validar_configuracion_auth(settings(jwt_algorithm=algoritmo))


def test_no_arranca_con_claves_de_servicio_cortas():
    with pytest.raises(RuntimeError, match="menos de 32"):
        validar_configuracion_auth(settings(claves_servicio="leads=corta"))


def test_no_arranca_con_claves_de_servicio_repetidas():
    repetida = "z" * 32
    with pytest.raises(RuntimeError, match="distinta"):
        validar_configuracion_auth(settings(claves_servicio=f"leads={repetida},calificacion={repetida}"))


def test_no_arranca_con_un_servicio_desconocido():
    with pytest.raises(RuntimeError, match="desconocidos"):
        validar_configuracion_auth(settings(claves_servicio="vehiculos=" + "a" * 32))


@pytest.mark.parametrize("texto", ["leads", "leads=", "=clave", "leads=a,,monitor"])
def test_claves_de_servicio_mal_escritas(texto):
    with pytest.raises(RuntimeError):
        validar_configuracion_auth(settings(claves_servicio=texto))


def test_sin_claves_de_servicio_arranca_pero_avisa(caplog):
    with caplog.at_level("WARNING"):
        validar_configuracion_auth(settings(claves_servicio=None))
    assert "CLAVES_SERVICIO" in caplog.text


def test_las_claves_se_leen_como_nombre_y_clave():
    assert settings().claves_servicio_por_nombre == {
        "leads": "a" * 32,
        "calificacion": "b" * 32,
        "ia-resumen": "c" * 32,
    }


def test_la_app_no_levanta_sin_secreto():
    app = crear_app(Settings(_env_file=None, kafka_enabled=False, database_url=None))
    with pytest.raises(RuntimeError, match="JWT_SECRET"):
        with TestClient(app):
            pass


def test_la_autenticacion_viene_habilitada_por_defecto():
    assert Settings(_env_file=None).auth_habilitada is True


# ------------------------------------------------------------------ revocaciones


async def test_un_token_revocado_deja_de_servir_hasta_que_vence():
    rev = Revocaciones()
    vence = datetime.now(timezone.utc) + timedelta(minutes=15)
    assert not rev.esta_revocado("jti-1")
    await rev.revocar("jti-1", vence)
    assert rev.esta_revocado("jti-1")
    assert not rev.esta_revocado("jti-2")


async def test_un_revocado_vencido_se_olvida():
    rev = Revocaciones()
    await rev.revocar("jti-viejo", datetime.now(timezone.utc) - timedelta(seconds=1))
    assert not rev.esta_revocado("jti-viejo")
    assert "jti-viejo" not in rev._expiraciones


async def test_sin_base_las_revocaciones_igual_funcionan_en_memoria():
    rev = Revocaciones()
    assert await rev.cargar() == 0


# ----------------------------------------------------------------------- política


@pytest.mark.parametrize(
    ("metodo", "recurso", "ruta", "headers", "publica"),
    [
        ("POST", "leads", "/leads", {}, True),
        ("POST", "leads", "/leads/", {}, True),
        ("GET", "leads", "/leads", {}, False),
        ("POST", "leads", "/leads/1", {}, False),
        ("GET", "vehiculos", "/vehiculos/1", {}, True),
        ("HEAD", "vehiculos", "/vehiculos", {}, True),
        ("POST", "vehiculos", "/vehiculos", {}, False),
        ("POST", "vehiculos", "/vehiculos", {"X-Api-Key": "k"}, True),
        ("PUT", "vehiculos", "/vehiculos/1/foto", {"x-api-key": "k"}, True),
        # La clave de otro servicio no abre rutas que no son de vehiculos.
        ("PATCH", "clientes", "/clientes/1", {"X-Api-Key": "k"}, False),
        ("GET", "clientes", "/clientes/1", {}, False),
        ("POST", "auth", "/auth/login", {}, True),
        ("POST", "auth", "/auth/refresh", {}, True),
        ("POST", "auth", "/auth/logout", {}, True),
        ("GET", "auth", "/auth/login", {}, False),
        ("GET", "auth", "/auth/me", {}, False),
        ("POST", "auth", "/auth/usuarios", {}, False),
        ("OPTIONS", "clientes", "/clientes/1", {}, True),
        ("GET", "monitor", "/monitor/estado", {}, False),
        ("GET", "resumen", "/resumen", {}, False),
        ("GET", "multas", "/multas/ABC123", {}, False),
        ("GET", "algo-que-no-existe", "/x", {}, False),
    ],
)
def test_politica_de_rutas(metodo, recurso, ruta, headers, publica):
    assert es_publica(metodo, recurso, ruta, headers) is publica
