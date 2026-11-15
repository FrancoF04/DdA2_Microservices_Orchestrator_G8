"""Flujo CONSULTA_MULTAS_VEHICULO y purga del historial, contra Postgres real.

Se saltean si no hay base (ver test_orquestacion_integracion.py). El scraper y vehiculos son dobles de prueba.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import httpx
import pytest
from sqlalchemy import delete, select

from app.config import Settings
from app.db import session as db
from app.db.models import EventoProcesado, Flujo, Outbox
from app.flujos import consulta_multas, despachador, multas, servicio
from app.main import crear_app
from app.proxy.clientes_http import ClientesHttp
from tests.test_orquestacion_integracion import (  # noqa: F401 - `base` y `event_loop_policy` son fixtures
    KafkaFalso,
    _despachar,
    _flujo,
    _hay_flujo,
    _outbox,
    base,
    evento,
    event_loop_policy,
    sobre,
    a_la_dlt,
)

PATENTE = "AB123CD"
ACTA = {
    "numeroActa": "A-1",
    "dominio": PATENTE,
    "importe": 33150,
    "importeSinDescuento": 39000,
    "tipoInfraccion": "LEY 24449 - Art. 77",
    "lugar": "Córdoba",
    "juzgado": "Juzgado 1",
}


class VehiculosFalso:
    """Transporte HTTP que responde como vehiculos: GET de una unidad, listado de disponibles y PUT de multas."""

    def __init__(self, patente: Optional[str] = PATENTE) -> None:
        self.patente = patente
        self.estado_get = 200
        self.estado_put = 204
        self.disponibles: list[str] = []
        self.pedidos: list[httpx.Request] = []

    def transporte(self) -> httpx.MockTransport:
        def manejar(request: httpx.Request) -> httpx.Response:
            self.pedidos.append(request)
            ruta = request.url.path
            if request.method == "PUT" and ruta.endswith("/multas"):
                return httpx.Response(self.estado_put)
            if request.method == "GET" and ruta == "/vehiculos":
                return httpx.Response(200, json=[{"id": v, "patente": self.patente} for v in self.disponibles])
            if request.method == "GET":
                if self.estado_get != 200:
                    return httpx.Response(self.estado_get)
                return httpx.Response(200, json={"id": ruta.rsplit("/", 1)[-1], "patente": self.patente})
            return httpx.Response(405)

        return httpx.MockTransport(manejar)

    def puts(self) -> list[httpx.Request]:
        return [p for p in self.pedidos if p.method == "PUT"]


def _unidad() -> str:
    return f"test-{uuid.uuid4()}"


def resultado(unidad: str, actas: int = 1) -> dict[str, Any]:
    return {"vehiculoId": unidad, "dominio": PATENTE, "infracciones": [ACTA] * actas, "subtotal": 39000 * actas}


def fallo(unidad: str, motivo: str = multas.SITIO_NO_DISPONIBLE, reintentable: bool = True) -> dict[str, Any]:
    return {"vehiculoId": unidad, "motivo": motivo, "reintentable": reintentable}


def _con_clave(settings: Settings) -> Settings:
    return settings.model_copy(update={"vehiculos_api_key": "clave-de-prueba"})


async def _abrir(settings: Settings, unidad: str, correlation_id: Optional[str] = "cid-multas") -> Flujo:
    async with db.obtener_sessionmaker()() as s, s.begin():
        return await consulta_multas.abrir_consulta(s, settings, unidad, correlation_id)


def _evento(tipo: str, payload: dict[str, Any]) -> dict[str, Any]:
    mensaje = sobre(tipo, {"leadId": payload["vehiculoId"], **payload})
    mensaje["payload"] = payload  # el sobre de multas no lleva leadId
    return mensaje


async def _consultar_y_responder(settings: Settings, unidad: str, kafka: KafkaFalso, http: ClientesHttp) -> None:
    """Abre la consulta, la publica y el scraper responde con una multa."""
    await _abrir(settings, unidad)
    await _despachar(settings, kafka, http)
    await evento(settings, _evento("MultasConsultadas", resultado(unidad)))


# ------------------------------------------------------------------ abrir la consulta


async def test_abrir_una_consulta_ordena_consultar_multas_sin_guardar_la_patente(base: Settings) -> None:
    unidad = _unidad()
    flujo = await _abrir(base, unidad)

    assert flujo.tipo == multas.TIPO and flujo.estado == multas.CONSULTANDO_MULTAS
    fila, = await _outbox(unidad)
    assert fila.topic == base.kafka_topico_comandos
    assert fila.mensaje == {
        "eventType": "ConsultarMultas",
        "destino": "multas",
        "vehiculoId": unidad,
        "correlationId": "cid-multas",
    }  # sin payload: la patente se consulta a vehiculos al publicar


async def test_no_se_abre_otra_consulta_mientras_hay_una_esperando(base: Settings) -> None:
    unidad = _unidad()
    await _abrir(base, unidad)

    with pytest.raises(multas.ConsultaEnCurso):
        await _abrir(base, unidad)

    assert len(await _outbox(unidad)) == 1


async def test_se_publica_con_el_vehiculo_y_su_patente_y_el_mismo_correlation_id(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    await _abrir(base, unidad)

    assert await _despachar(base, kafka, ClientesHttp(vehiculos.transporte())) == 1

    (topico, clave, mensaje), = kafka.publicados
    assert (topico, clave) == (base.kafka_topico_comandos, unidad)
    assert mensaje["eventType"] == "ConsultarMultas" and mensaje["destino"] == "multas"
    assert mensaje["payload"] == {"vehiculoId": unidad, "patente": PATENTE}
    assert mensaje["correlationId"] == "cid-multas"


async def test_una_unidad_sin_patente_no_se_reintenta_y_deja_el_flujo_fallido(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso(patente=None)
    await _abrir(base, unidad)

    await _despachar(base, kafka, ClientesHttp(vehiculos.transporte()))

    fila, = await _outbox(unidad)
    assert fila.abandonado_en is not None and fila.intentos == base.outbox_max_intentos
    assert kafka.publicados == []
    flujo = await _flujo(unidad)
    assert flujo.estado == multas.FALLIDO and "patente" in flujo.ultimo_error
    assert flujo.pasos[-1].estado == multas.ERROR


async def test_una_unidad_que_ya_no_existe_tampoco_se_reintenta(base: Settings) -> None:
    unidad = _unidad()
    vehiculos = VehiculosFalso()
    vehiculos.estado_get = 404
    await _abrir(base, unidad)

    await _despachar(base, KafkaFalso(), ClientesHttp(vehiculos.transporte()))

    assert (await _flujo(unidad)).estado == multas.FALLIDO
    assert len(vehiculos.pedidos) == 1


async def test_si_vehiculos_esta_caido_el_comando_espera_y_se_reintenta(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    vehiculos.estado_get = 503
    await _abrir(base, unidad)

    await _despachar(base, kafka, ClientesHttp(vehiculos.transporte()))
    fila, = await _outbox(unidad)
    assert fila.intentos == 1 and fila.abandonado_en is None
    assert (await _flujo(unidad)).estado == multas.CONSULTANDO_MULTAS

    vehiculos.estado_get = 200
    await _despachar(base, kafka, ClientesHttp(vehiculos.transporte()), ahora=datetime.now(timezone.utc) + timedelta(days=1))
    assert len(kafka.publicados) == 1


# ------------------------------------------------------------------ el resultado


async def test_el_resultado_se_entrega_a_vehiculos_y_el_flujo_termina(base: Settings) -> None:
    unidad = _unidad()
    cfg = _con_clave(base)
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _consultar_y_responder(cfg, unidad, kafka, http)

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.ACTUALIZANDO_VEHICULO
    pendiente = [f for f in await _outbox(unidad) if f.publicado_en is None]
    assert [f.mensaje["eventType"] for f in pendiente] == ["GuardarMultas"]
    assert pendiente[0].mensaje["payload"]["infracciones"] == [ACTA]
    assert "vehiculoId" not in pendiente[0].mensaje["payload"]

    await _despachar(cfg, kafka, http)

    put, = vehiculos.puts()
    assert put.url.path == f"/vehiculos/{unidad}/multas"
    assert put.headers["X-Api-Key"] == "clave-de-prueba"
    assert put.headers["X-Correlation-Id"] == "cid-multas"
    assert httpx.Response(200, content=put.content).json()["infracciones"] == [ACTA]
    flujo = await _flujo(unidad)
    assert flujo.estado == multas.COMPLETADO and flujo.finalizado_en is not None
    assert [(p.paso, p.estado) for p in flujo.pasos] == [("CONSULTAR_MULTAS", "OK"), ("GUARDAR_MULTAS", "OK")]
    # ya entregado: el resultado (dato personal) no queda en el outbox
    assert all("payload" not in f.mensaje for f in await _outbox(unidad))


async def test_una_unidad_sin_multas_tambien_se_entrega(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)

    await evento(base, _evento("MultasConsultadas", resultado(unidad, actas=0)))
    await _despachar(base, kafka, http)

    assert len(vehiculos.puts()) == 1
    assert (await _flujo(unidad)).estado == multas.COMPLETADO


async def test_un_resultado_repetido_no_se_entrega_dos_veces(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _consultar_y_responder(base, unidad, kafka, http)

    await evento(base, _evento("MultasConsultadas", resultado(unidad)))  # otro eventId, mismo resultado
    await _despachar(base, kafka, http)
    await evento(base, _evento("MultasConsultadas", resultado(unidad)))
    await _despachar(base, kafka, http)

    assert len(vehiculos.puts()) == 1


async def test_el_mismo_evento_dos_veces_se_procesa_una_sola(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)

    mensaje = _evento("MultasConsultadas", resultado(unidad))
    await evento(base, mensaje)
    await evento(base, mensaje)

    guardados = [f for f in await _outbox(unidad) if f.mensaje["eventType"] == "GuardarMultas"]
    assert len(guardados) == 1


async def test_un_resultado_de_una_unidad_sin_consulta_abierta_se_ignora(base: Settings) -> None:
    unidad = _unidad()

    await evento(base, _evento("MultasConsultadas", resultado(unidad)))

    assert not await _hay_flujo(unidad)
    assert await _outbox(unidad) == []


async def test_si_vehiculos_esta_caido_el_resultado_espera_y_se_reintenta(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _consultar_y_responder(base, unidad, kafka, http)

    vehiculos.estado_put = 503
    await _despachar(base, kafka, http)
    assert (await _flujo(unidad)).estado == multas.ACTUALIZANDO_VEHICULO
    pendiente, = [f for f in await _outbox(unidad) if f.publicado_en is None]
    assert pendiente.intentos == 1 and pendiente.mensaje["payload"]["infracciones"] == [ACTA]

    vehiculos.estado_put = 204
    await _despachar(base, kafka, http, ahora=datetime.now(timezone.utc) + timedelta(days=1))
    assert (await _flujo(unidad)).estado == multas.COMPLETADO


async def test_si_vehiculos_rechaza_el_resultado_el_flujo_queda_fallido_sin_reintentar(base: Settings) -> None:
    unidad = _unidad()
    kafka, vehiculos = KafkaFalso(), VehiculosFalso()
    http = ClientesHttp(vehiculos.transporte())
    await _consultar_y_responder(base, unidad, kafka, http)

    vehiculos.estado_put = 422
    await _despachar(base, kafka, http)

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.FALLIDO and "422" in flujo.ultimo_error
    assert len(vehiculos.puts()) == 1
    assert all("payload" not in f.mensaje for f in await _outbox(unidad))  # abandonado: sin datos


# ------------------------------------------------------------------ fallas del scraper


async def test_un_error_reintentable_vuelve_a_pedir_la_consulta_con_espera_creciente(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)

    await evento(base, _evento("ConsultaMultasFallida", fallo(unidad)))

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.CONSULTANDO_MULTAS and flujo.intentos == 1
    assert [(p.paso, p.estado, p.intento) for p in flujo.pasos] == [
        ("CONSULTAR_MULTAS", "ERROR", 1),
        ("CONSULTAR_MULTAS", "PENDIENTE", 2),
    ]
    nueva = [f for f in await _outbox(unidad) if f.publicado_en is None][0]
    assert nueva.mensaje["eventType"] == "ConsultarMultas"
    espera = (nueva.proximo_intento_en - datetime.now(timezone.utc)).total_seconds()
    assert 50 < espera <= base.multas_espera_base_segundos  # 60 s la primera vez
    # todavía no le toca: no sale hasta que pase la espera
    assert await _despachar(base, kafka, http) == 0
    assert await _despachar(base, kafka, http, ahora=datetime.now(timezone.utc) + timedelta(minutes=5)) == 1


async def test_la_segunda_espera_se_duplica(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)
    await evento(base, _evento("ConsultaMultasFallida", fallo(unidad)))
    await _despachar(base, kafka, http, ahora=datetime.now(timezone.utc) + timedelta(minutes=5))

    await evento(base, _evento("ConsultaMultasFallida", fallo(unidad)))

    nueva = [f for f in await _outbox(unidad) if f.publicado_en is None][0]
    espera = (nueva.proximo_intento_en - datetime.now(timezone.utc)).total_seconds()
    assert 110 < espera <= 2 * base.multas_espera_base_segundos  # 120 s
    assert (await _flujo(unidad)).intentos == 2


async def test_agotados_los_intentos_el_flujo_queda_fallido(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    for n in range(base.multas_max_intentos):
        await _despachar(base, kafka, http, ahora=datetime.now(timezone.utc) + timedelta(days=n + 1))
        await evento(base, _evento("ConsultaMultasFallida", fallo(unidad)))

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.FALLIDO and "3 intentos" in flujo.ultimo_error
    consultas = [s for s in kafka.publicados if s[2]["eventType"] == "ConsultarMultas"]
    assert len(consultas) == base.multas_max_intentos  # ni una más
    assert [f for f in await _outbox(unidad) if f.publicado_en is None] == []


async def test_un_error_no_reintentable_deja_el_flujo_fallido_sin_volver_a_pedir(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)

    await evento(base, _evento("ConsultaMultasFallida", fallo(unidad, multas.ESTRUCTURA_NO_RECONOCIDA, False)))

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.FALLIDO
    assert "ESTRUCTURA_NO_RECONOCIDA" in flujo.ultimo_error and "no se reintenta" in flujo.ultimo_error
    assert [f for f in await _outbox(unidad) if f.publicado_en is None] == []


async def test_un_fallo_sin_decir_si_es_reintentable_no_se_reintenta(base: Settings) -> None:
    unidad = _unidad()
    await _abrir(base, unidad)

    await evento(base, _evento("ConsultaMultasFallida", {"vehiculoId": unidad, "motivo": "ALGO"}))

    assert (await _flujo(unidad)).estado == multas.FALLIDO


async def test_si_el_comando_llega_a_la_dlt_el_flujo_queda_fallido(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)
    comando = kafka.publicados[0][2]  # tal como lo vio el scraper

    await a_la_dlt(base, comando)

    flujo = await _flujo(unidad)
    assert flujo.estado == multas.FALLIDO and "DLT" in flujo.ultimo_error


async def test_un_fallo_del_scraper_de_otro_ciclo_no_pisa_una_consulta_terminada(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _consultar_y_responder(base, unidad, kafka, http)
    await _despachar(base, kafka, http)

    await evento(base, _evento("ConsultaMultasFallida", fallo(unidad)))

    assert (await _flujo(unidad)).estado == multas.COMPLETADO
    assert [f for f in await _outbox(unidad) if f.publicado_en is None] == []


# ------------------------------------------------------------------ vencimiento y reapertura


async def test_si_el_scraper_no_responde_el_flujo_expira_y_la_respuesta_tardia_lo_recupera(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _abrir(base, unidad)
    await _despachar(base, kafka, http)

    await servicio.vencer_flujos(ahora=datetime.now(timezone.utc) + timedelta(days=1))
    flujo = await _flujo(unidad)
    assert flujo.estado == multas.EXPIRADO and "no llegó MultasConsultadas" in flujo.ultimo_error
    assert flujo.pasos[0].estado == multas.TIMEOUT

    await evento(base, _evento("MultasConsultadas", resultado(unidad)))
    assert (await _flujo(unidad)).estado == multas.ACTUALIZANDO_VEHICULO


async def test_una_consulta_terminada_se_puede_abrir_de_nuevo_y_conserva_el_historial(base: Settings) -> None:
    unidad = _unidad()
    kafka, http = KafkaFalso(), ClientesHttp(VehiculosFalso().transporte())
    await _consultar_y_responder(base, unidad, kafka, http)
    await _despachar(base, kafka, http)
    assert (await _flujo(unidad)).estado == multas.COMPLETADO

    flujo = await _abrir(base, unidad, "cid-segunda")

    assert flujo.estado == multas.CONSULTANDO_MULTAS and flujo.finalizado_en is None
    assert flujo.correlation_id == "cid-segunda"
    assert [p.paso for p in (await _flujo(unidad)).pasos].count("CONSULTAR_MULTAS") == 2
    assert [f.mensaje["eventType"] for f in await _outbox(unidad) if f.publicado_en is None] == ["ConsultarMultas"]


# ------------------------------------------------------------------ barrido


async def test_el_barrido_abre_una_consulta_por_unidad_disponible_sin_repetir_las_que_estan_en_curso(
    base: Settings,
) -> None:
    a, b, c = _unidad(), _unidad(), _unidad()
    vehiculos = VehiculosFalso()
    vehiculos.disponibles = [a, b, c]
    await _abrir(base, b)  # b ya tiene una consulta esperando
    http = ClientesHttp(vehiculos.transporte())

    abiertas = await consulta_multas.barrer_multas(base, http)

    assert abiertas == 2
    assert len(await _outbox(a)) == 1 and len(await _outbox(b)) == 1 and len(await _outbox(c)) == 1
    consulta, = [p for p in vehiculos.pedidos]
    assert consulta.url.params["disponible"] == "true"


async def test_el_barrido_respeta_el_tope_de_consultas(base: Settings) -> None:
    unidades = [_unidad() for _ in range(4)]
    vehiculos = VehiculosFalso()
    vehiculos.disponibles = unidades
    cfg = base.model_copy(update={"multas_barrido_maximo": 3})

    abiertas = await consulta_multas.barrer_multas(cfg, ClientesHttp(vehiculos.transporte()))

    assert abiertas == 3
    assert not await _hay_flujo(unidades[3])


# ------------------------------------------------------------------ endpoint


async def test_el_endpoint_abre_la_consulta_y_responde_409_si_ya_hay_una(base: Settings) -> None:
    unidad = str(uuid.uuid4())
    app = crear_app(base)  # sin lifespan: la base ya la abrió la fixture
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://orq") as cliente:
            primera = await cliente.post(f"/api/orquestador/multas/{unidad}")
            segunda = await cliente.post(f"/api/orquestador/multas/{unidad}")
            invalida = await cliente.post("/api/orquestador/multas/no-es-un-uuid")

        assert primera.status_code == 202
        cuerpo = primera.json()
        assert cuerpo["tipo"] == multas.TIPO and cuerpo["estado"] == multas.CONSULTANDO_MULTAS
        assert cuerpo["claveNegocio"] == unidad and cuerpo["correlationId"]
        assert segunda.status_code == 409 and segunda.json()["codigo"] == "CONSULTA_EN_CURSO"
        assert invalida.status_code == 422
        assert len(await _outbox(unidad)) == 1
    finally:
        async with db.obtener_sessionmaker()() as s, s.begin():
            await s.execute(delete(Outbox).where(Outbox.clave == unidad))
            await s.execute(delete(Flujo).where(Flujo.clave_negocio == unidad))


# ------------------------------------------------------------------ purga


async def test_la_purga_borra_lo_resuelto_y_viejo_pero_no_lo_pendiente_ni_lo_reciente(base: Settings) -> None:
    unidad = _unidad()
    ahora = datetime.now(timezone.utc)
    viejo = ahora - timedelta(days=base.retencion_historial_dias + 1)
    reciente = ahora - timedelta(days=1)
    async with db.obtener_sessionmaker()() as s, s.begin():
        flujo = Flujo(tipo=multas.TIPO, clave_negocio=unidad, estado=multas.COMPLETADO)
        s.add(flujo)
        await s.flush()

        def fila(**campos: Any) -> Outbox:
            return Outbox(flujo_id=flujo.id, topic="t", clave=unidad, mensaje={"eventType": "X"}, **campos)

        s.add_all(
            [
                fila(publicado_en=viejo),  # 0: se va
                fila(abandonado_en=viejo),  # 1: se va
                fila(publicado_en=reciente),  # 2: queda
                fila(),  # 3: pendiente, queda
            ]
        )
        s.add_all(
            [
                EventoProcesado(clave=f"test-viejo-{unidad}", topic="t", procesado_en=viejo),
                EventoProcesado(clave=f"test-reciente-{unidad}", topic="t", procesado_en=reciente),
            ]
        )

    borradas, eventos = await despachador.purgar_historial(base, ahora)

    restantes = await _outbox(unidad)
    assert len(restantes) == 2
    assert sorted(f.publicado_en is None for f in restantes) == [False, True]
    assert borradas >= 2 and eventos >= 1
    async with db.obtener_sessionmaker()() as s:
        claves = set((await s.execute(select(EventoProcesado.clave).where(EventoProcesado.clave.like("test-%")))).scalars())
    assert f"test-reciente-{unidad}" in claves and f"test-viejo-{unidad}" not in claves


# ------------------------------------------------------------------ alta de una unidad por el proxy


def _gateway(settings: Settings, estado: int, location: Optional[str]) -> Any:
    """La app con un vehiculos simulado que responde al `POST /vehiculos`."""

    def manejar(request: httpx.Request) -> httpx.Response:
        cabeceras = {"Location": location} if location else {}
        return httpx.Response(estado, json={"id": "x"}, headers=cabeceras)

    app = crear_app(settings)  # sin lifespan: la base ya la abrió la fixture
    app.state.http = ClientesHttp(httpx.MockTransport(manejar))
    return app


async def _alta(app: Any, ruta: str = "/api/vehiculos", metodo: str = "POST") -> httpx.Response:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://orq") as cliente:
        return await cliente.request(metodo, ruta, json={"marca": "Toyota"})


async def _esperar_flujo(unidad: str, segundos: float = 3.0) -> bool:
    import asyncio

    fin = asyncio.get_event_loop().time() + segundos
    while asyncio.get_event_loop().time() < fin:
        if await _hay_flujo(unidad):
            return True
        await asyncio.sleep(0.05)
    return False


async def test_el_alta_de_una_unidad_abre_la_consulta_de_multas_si_esta_activado(base: Settings) -> None:
    unidad = str(uuid.uuid4())
    cfg = base.model_copy(update={"multas_al_alta": True})
    try:
        respuesta = await _alta(_gateway(cfg, 201, f"http://vehiculos.test/vehiculos/{unidad}"))

        assert respuesta.status_code == 201  # la respuesta al cliente no cambia
        assert await _esperar_flujo(unidad)
        flujo = await _flujo(unidad)
        assert flujo.tipo == multas.TIPO and flujo.estado == multas.CONSULTANDO_MULTAS
        fila, = await _outbox(unidad)
        assert fila.mensaje["eventType"] == "ConsultarMultas"
    finally:
        async with db.obtener_sessionmaker()() as s, s.begin():
            await s.execute(delete(Outbox).where(Outbox.clave == unidad))
            await s.execute(delete(Flujo).where(Flujo.clave_negocio == unidad))


async def test_con_el_alta_desactivada_no_se_abre_nada(base: Settings) -> None:
    unidad = str(uuid.uuid4())
    assert base.multas_al_alta is False
    respuesta = await _alta(_gateway(base, 201, f"http://vehiculos.test/vehiculos/{unidad}"))

    assert respuesta.status_code == 201
    assert not await _esperar_flujo(unidad, segundos=0.5)


@pytest.mark.parametrize(
    "estado, metodo, ruta, location",
    [
        (422, "POST", "/api/vehiculos", "http://vehiculos.test/vehiculos/{u}"),  # el alta falló
        (200, "PUT", "/api/vehiculos/{u}", None),  # no es un alta
        (201, "POST", "/api/vehiculos/{u}/foto", "http://vehiculos.test/vehiculos/{u}"),  # no es el alta de una unidad
        (201, "POST", "/api/vehiculos", None),  # sin Location no hay id
        (201, "POST", "/api/vehiculos", "http://vehiculos.test/vehiculos/no-es-un-uuid"),
    ],
)
async def test_solo_el_alta_exitosa_de_una_unidad_abre_la_consulta(
    base: Settings, estado: int, metodo: str, ruta: str, location: Optional[str]
) -> None:
    unidad = str(uuid.uuid4())
    cfg = base.model_copy(update={"multas_al_alta": True})
    ruta = ruta.format(u=unidad)
    location = location.format(u=unidad) if location else None

    await _alta(_gateway(cfg, estado, location), ruta, metodo)

    assert not await _esperar_flujo(unidad, segundos=0.5)


async def test_si_ya_hay_una_consulta_en_curso_el_alta_no_abre_otra_ni_falla(base: Settings) -> None:
    unidad = str(uuid.uuid4())
    cfg = base.model_copy(update={"multas_al_alta": True})
    try:
        await _abrir(cfg, unidad)
        respuesta = await _alta(_gateway(cfg, 201, f"http://vehiculos.test/vehiculos/{unidad}"))

        assert respuesta.status_code == 201
        import asyncio

        await asyncio.sleep(0.3)
        assert len(await _outbox(unidad)) == 1
    finally:
        async with db.obtener_sessionmaker()() as s, s.begin():
            await s.execute(delete(Outbox).where(Outbox.clave == unidad))
            await s.execute(delete(Flujo).where(Flujo.clave_negocio == unidad))
