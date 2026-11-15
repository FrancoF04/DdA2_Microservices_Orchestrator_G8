from __future__ import annotations

import json
from collections.abc import Callable, Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

from app.auditoria import Llamada
from app.config import Settings
from app.main import crear_app
from app.middleware.correlation_id import HEADER


class AuditoriaFalsa:
    """Junta en memoria lo que el proxy manda a auditar."""

    def __init__(self) -> None:
        self.llamadas: list[Llamada] = []

    def registrar(self, llamada: Llamada) -> None:
        self.llamadas.append(llamada)

    async def iniciar(self) -> None: ...

    async def detener(self) -> None: ...


class ServiciosFalsos:
    """Simula los servicios destino; cada test define cómo responden."""

    def __init__(self) -> None:
        self.recibidas: list[httpx.Request] = []
        self.responder: Callable[[httpx.Request], httpx.Response] = lambda r: httpx.Response(
            200, json={"ok": True}
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        request.read()
        self.recibidas.append(request)
        return self.responder(request)


@pytest.fixture
def servicios() -> ServiciosFalsos:
    return ServiciosFalsos()


@pytest.fixture
def auditoria() -> AuditoriaFalsa:
    return AuditoriaFalsa()


@pytest.fixture
def gateway(servicios: ServiciosFalsos, auditoria: AuditoriaFalsa) -> Iterator[TestClient]:
    settings = Settings(
        _env_file=None,
        kafka_enabled=False,
        database_url=None,
        cors_allowed_origins="http://front.test",
        leads_url="http://leads.test",
        vehiculos_url="http://vehiculos.test/",
        calificacion_url="http://calificacion.test",
        auth_url=None,
        proxy_reintentos_get=1,
        auth_habilitada=False,  # la autenticación se prueba en test_auth_gateway.py
    )
    app = crear_app(settings, transporte_http=httpx.MockTransport(servicios))
    app.state.auditoria = auditoria
    with TestClient(app) as client:
        yield client


# ------------------------------------------------------------------ ruteo


def test_reenvia_get_con_query_al_servicio(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    servicios.responder = lambda r: httpx.Response(200, json=[{"id": "L1"}])

    respuesta = gateway.get("/api/leads?estado=CALIFICADO&nivel=MUY_POTABLE")

    assert respuesta.status_code == 200
    assert respuesta.json() == [{"id": "L1"}]
    enviada = servicios.recibidas[0]
    assert enviada.method == "GET"
    assert str(enviada.url) == "http://leads.test/leads?estado=CALIFICADO&nivel=MUY_POTABLE"


def test_mapea_calificaciones_al_servicio_calificacion(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    gateway.get("/api/calificaciones/L1")
    assert str(servicios.recibidas[0].url) == "http://calificacion.test/calificaciones/L1"


def test_url_base_con_barra_final_no_duplica(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    gateway.get("/api/vehiculos/V1/precio")
    assert str(servicios.recibidas[0].url) == "http://vehiculos.test/vehiculos/V1/precio"


def test_reenvia_patch_con_query(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    gateway.patch("/api/leads/L1/state?nuevo_estado=EN_GESTION")
    enviada = servicios.recibidas[0]
    assert enviada.method == "PATCH"
    assert str(enviada.url) == "http://leads.test/leads/L1/state?nuevo_estado=EN_GESTION"


def test_reenvia_body_y_content_type(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    servicios.responder = lambda r: httpx.Response(202, json={"lead_id": "L9"})
    body = {"cliente": {"nombre": "Martina"}, "vehiculoId": "V1", "anticipo": 3000000}

    respuesta = gateway.post("/api/leads", json=body)

    assert respuesta.status_code == 202
    assert respuesta.json() == {"lead_id": "L9"}
    enviada = servicios.recibidas[0]
    assert json.loads(enviada.content) == body
    assert enviada.headers["content-type"] == "application/json"


def test_respuesta_de_error_del_servicio_pasa_tal_cual(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    problema = {"status": 422, "codigo": "VALIDACION", "errores": [{"campo": "cuil"}]}
    servicios.responder = lambda r: httpx.Response(
        422, json=problema, headers={"content-type": "application/problem+json"}
    )

    respuesta = gateway.post("/api/leads", json={})

    assert respuesta.status_code == 422
    assert respuesta.json() == problema
    assert respuesta.headers["content-type"] == "application/problem+json"


def test_recurso_desconocido_da_404(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    respuesta = gateway.get("/api/inexistente/1")
    assert respuesta.status_code == 404
    assert respuesta.json()["codigo"] == "RECURSO_NO_ENCONTRADO"
    assert servicios.recibidas == []


def test_servicio_sin_url_configurada_da_503(gateway: TestClient) -> None:
    respuesta = gateway.post("/api/auth/login", json={})
    assert respuesta.status_code == 503
    assert respuesta.json()["codigo"] == "SERVICIO_NO_CONFIGURADO"
    assert respuesta.json()["servicio"] == "auth"


def test_rutas_propias_no_se_reenvian(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    respuesta = gateway.get("/api/orquestador/servicios")
    assert respuesta.json()["codigo"] == "BASE_NO_CONFIGURADA"
    assert servicios.recibidas == []


# ------------------------------------------------------------------ headers


def test_propaga_el_correlation_id(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    servicios.responder = lambda r: httpx.Response(200, headers={HEADER: "otro-del-servicio"})

    respuesta = gateway.get("/api/leads", headers={HEADER: "cid-123"})

    assert servicios.recibidas[0].headers[HEADER] == "cid-123"
    assert respuesta.headers.get_list(HEADER) == ["cid-123"]


def test_no_reenvia_hop_by_hop_y_agrega_forwarded(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    gateway.get(
        "/api/leads",
        headers={"Keep-Alive": "timeout=5", "Proxy-Authorization": "secreto", "X-Api-Key": "clave"},
    )

    enviada = servicios.recibidas[0].headers
    assert enviada["host"] == "leads.test"
    assert enviada["x-api-key"] == "clave"  # pass-through de la clave de servicio
    assert enviada["x-forwarded-proto"] == "http"
    assert "keep-alive" not in enviada
    assert "proxy-authorization" not in enviada


def test_saca_los_headers_cors_del_servicio(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    servicios.responder = lambda r: httpx.Response(
        200,
        json={},
        headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Credentials": "true"},
    )

    respuesta = gateway.get("/api/leads", headers={"Origin": "http://front.test"})

    # Queda solo el del orquestador, con el origen permitido.
    assert respuesta.headers.get_list("access-control-allow-origin") == ["http://front.test"]


def test_preflight_cors_lo_responde_el_orquestador(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    respuesta = gateway.options(
        "/api/leads",
        headers={"Origin": "http://front.test", "Access-Control-Request-Method": "POST"},
    )

    assert respuesta.status_code == 200
    assert respuesta.headers["access-control-allow-origin"] == "http://front.test"
    assert servicios.recibidas == []


def test_origen_no_permitido_no_recibe_cors(gateway: TestClient) -> None:
    respuesta = gateway.get("/api/leads", headers={"Origin": "http://malicioso.test"})
    assert "access-control-allow-origin" not in respuesta.headers


# ------------------------------------------------------------------ errores de red


def test_servicio_caido_da_503_y_reintenta_get(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    def caido(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused", request=request)

    servicios.responder = caido

    respuesta = gateway.get("/api/leads")

    assert respuesta.status_code == 503
    assert respuesta.json()["codigo"] == "SERVICIO_NO_DISPONIBLE"
    assert respuesta.json()["servicio"] == "leads"
    assert len(servicios.recibidas) == 2  # 1 intento + 1 reintento


def test_post_no_se_reintenta(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    def caido(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("Connection refused", request=request)

    servicios.responder = caido

    respuesta = gateway.post("/api/leads", json={"a": 1})

    assert respuesta.status_code == 503
    assert len(servicios.recibidas) == 1


def test_servicio_lento_da_504(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    def lento(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout", request=request)

    servicios.responder = lento

    respuesta = gateway.get("/api/leads")

    assert respuesta.status_code == 504
    assert respuesta.json()["codigo"] == "SERVICIO_TIMEOUT"


def test_conexion_cortada_da_502(gateway: TestClient, servicios: ServiciosFalsos) -> None:
    def corta(request: httpx.Request) -> httpx.Response:
        raise httpx.RemoteProtocolError("cortó", request=request)

    servicios.responder = corta

    assert gateway.get("/api/leads").json()["codigo"] == "SERVICIO_RECHAZO"


def test_get_se_reintenta_si_se_corto_una_conexion_reusada(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    llamadas = {"n": 0}

    def corta_la_primera(request: httpx.Request) -> httpx.Response:
        llamadas["n"] += 1
        if llamadas["n"] == 1:
            raise httpx.RemoteProtocolError("Server disconnected", request=request)
        return httpx.Response(200, json={"ok": True})

    servicios.responder = corta_la_primera

    assert gateway.get("/api/leads").status_code == 200
    assert len(servicios.recibidas) == 2


def test_timeout_reinicia_el_cliente_del_servicio(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    clientes = gateway.app.state.http
    gateway.get("/api/vehiculos/V1")  # crea los clientes de ambos servicios
    gateway.get("/api/leads")
    cliente_leads, cliente_vehiculos = clientes.para("leads"), clientes.para("vehiculos")

    def lento(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout", request=request)

    servicios.responder = lento
    gateway.get("/api/leads")

    assert clientes.para("leads") is not cliente_leads  # se reemplazó
    assert clientes.para("vehiculos") is cliente_vehiculos  # el resto no se toca


def test_pide_sin_comprimir_si_el_cliente_no_pidio_compresion(
    gateway: TestClient, servicios: ServiciosFalsos
) -> None:
    gateway.get("/api/leads", headers={"Accept-Encoding": ""})
    gateway.get("/api/leads", headers={"Accept-Encoding": "gzip"})

    assert servicios.recibidas[0].headers["accept-encoding"] in ("", "identity")
    assert servicios.recibidas[1].headers["accept-encoding"] == "gzip"


# ------------------------------------------------------------------ auditoría


def test_audita_la_llamada_sin_body(
    gateway: TestClient, servicios: ServiciosFalsos, auditoria: AuditoriaFalsa
) -> None:
    servicios.responder = lambda r: httpx.Response(201, json={})

    gateway.post(
        "/api/leads?x=1",
        json={"cliente": {"cuil": "20123456786"}},
        headers={HEADER: "cid-a", "Origin": "http://front.test"},
    )

    [llamada] = auditoria.llamadas
    assert llamada.destino == "leads"
    assert llamada.origen == "front"
    assert llamada.metodo == "POST"
    assert llamada.ruta == "/leads"  # sin query string
    assert llamada.codigo_http == 201
    assert llamada.correlation_id == "cid-a"
    assert llamada.error is None
    assert "20123456786" not in repr(llamada)  # nunca se guarda el body


def test_audita_errores_de_red(
    gateway: TestClient, servicios: ServiciosFalsos, auditoria: AuditoriaFalsa
) -> None:
    def lento(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timeout", request=request)

    servicios.responder = lento

    gateway.get("/api/calificaciones/L1", headers={"X-Origen-Servicio": "Leads"})

    [llamada] = auditoria.llamadas
    assert llamada.destino == "calificacion"
    assert llamada.origen == "leads"
    assert llamada.codigo_http is None
    assert llamada.error == "SERVICIO_TIMEOUT"
