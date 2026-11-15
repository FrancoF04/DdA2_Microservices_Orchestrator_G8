"""Tanda A: el orquestador publica `resumen-solicitado` (outbox + despachador), contra Postgres real.

Se saltean si no hay base: definir TEST_DATABASE_URL o tener levantado `docker compose up -d postgres`
con las migraciones aplicadas (`alembic upgrade head`).
"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import count
from typing import Any, Optional

import httpx
import pytest
from sqlalchemy import delete, select, text
from sqlalchemy.orm import selectinload

from app.config import Settings
from app.db import session as db
from app.db.models import EventoProcesado, Flujo, Outbox
from app.flujos import despachador
from app.flujos import registro_lead as rl
from app.flujos import servicio
from app.flujos.resumen import armar_payload_resumen
from app.kafka.cliente import MANEJADORES
from app.kafka.eventos import armar_sobre
from app.main import crear_app
from app.proxy.clientes_http import ClientesHttp

URL = os.getenv("TEST_DATABASE_URL", "postgresql://orquestador:orquestador@127.0.0.1:5432/postgres")
# Offsets al azar: la clave de idempotencia de lead-calificado (sin sobre) es topic:partition:offset.
_offsets = count(random.randint(10**9, 10**12))
_claves_usadas: list[str] = []


@pytest.fixture(scope="session")
def event_loop_policy():
    # psycopg async no funciona con el loop por defecto de Windows.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@dataclass
class Registro:
    topic: str
    partition: int
    offset: int
    timestamp: int


def registro(topico: str, momento: datetime) -> Registro:
    offset = next(_offsets)
    _claves_usadas.append(f"{topico}:0:{offset}")
    return Registro(topico, 0, offset, int(momento.timestamp() * 1000))


def _settings(**cambios: Any) -> Settings:
    valores: dict[str, Any] = dict(
        _env_file=None,
        database_url=URL,
        kafka_enabled=False,
        auth_habilitada=False,
        orquestador_publica_resumen=True,
        leads_url="http://leads.test",
        outbox_max_intentos=3,
        outbox_espera_base_segundos=10.0,
    )
    valores.update(cambios)
    return Settings(**valores)


@pytest.fixture
async def base():
    settings = _settings()
    db.iniciar_db(settings)
    listo = False
    try:
        if not await db.verificar_db(timeout_segundos=2):
            pytest.skip("No hay Postgres disponible para tests de integración.")
        async with db.obtener_sessionmaker()() as s:
            columna = (
                await s.execute(text("select to_regclass('orquestador.outbox')"))
            ).scalar()
            tiene_flujo_id = (
                await s.execute(
                    text(
                        "select 1 from information_schema.columns where table_schema = 'orquestador' "
                        "and table_name = 'outbox' and column_name = 'flujo_id'"
                    )
                )
            ).scalar()
        if columna is None or tiene_flujo_id is None:
            pytest.skip("Falta la migración 0005 (alembic upgrade head).")
        listo = True
        MANEJADORES.clear()
        servicio.registrar_manejadores(settings)
        yield settings
    finally:
        MANEJADORES.clear()
        if listo:
            # Los tests dejan solo filas propias (leads "test-..."); las limpia cada test, pero por si
            # alguno se cortó a la mitad se barren acá.
            async with db.obtener_sessionmaker()() as s, s.begin():
                await s.execute(delete(Outbox).where(Outbox.clave.like("test-%")))
                await s.execute(delete(Flujo).where(Flujo.clave_negocio.like("test-%")))
                await s.execute(delete(EventoProcesado).where(EventoProcesado.clave.in_(_claves_usadas)))
        await db.cerrar_db()


# ------------------------------------------------------------------ dobles de prueba


class KafkaFalso:
    def __init__(self, disponible: bool = True, falla: Optional[Exception] = None) -> None:
        self.disponible = disponible
        self.falla = falla
        self.publicados: list[tuple[str, str, dict[str, Any]]] = []

    async def publicar(self, topico, clave, event_type, payload, correlation_id=None):
        if self.falla is not None:
            raise self.falla
        sobre = armar_sobre(event_type, payload, correlation_id)
        self.publicados.append((topico, clave, sobre))
        return sobre


def lead_de(lead_id: str) -> dict[str, Any]:
    return {
        "id": lead_id,
        "cliente_id": "22222222-2222-2222-2222-222222222222",
        "vehiculo_id": "33333333-3333-3333-3333-333333333333",
        "anticipo": 1500000.0,
        "solicita_financiacion": True,
        "permuta_tiene": False,
        "permuta_descripcion": None,
        "permuta_valor_declarado": None,
        "provincia": "Córdoba",
        "localidad": None,
        "nivel": "POTABLE",
    }


class LeadsFalso:
    """Transporte HTTP que responde como `GET /leads/{id}` de leads."""

    def __init__(self) -> None:
        self.caido = False
        self.pedidos: list[httpx.Request] = []

    def transporte(self) -> httpx.MockTransport:
        def manejar(request: httpx.Request) -> httpx.Response:
            self.pedidos.append(request)
            if self.caido:
                raise httpx.ConnectError("leads caído", request=request)
            lead_id = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json=lead_de(lead_id))

        return httpx.MockTransport(manejar)


# ------------------------------------------------------------------ utilidades


async def _calificar(settings: Settings, lead: str, origen: str = "AUTOMATICA", cid: str = "cid-test") -> None:
    t0 = datetime.now(timezone.utc)
    pre, calif = settings.kafka_topico_preformulario_completado, settings.kafka_topico_lead_calificado
    await MANEJADORES[pre]({"eventId": f"{lead}-pre", "correlationId": cid, "payload": {"leadId": lead}}, registro(pre, t0))
    await MANEJADORES[calif]({"leadId": lead, "origen": origen}, registro(calif, t0 + timedelta(seconds=1)))


async def _flujo(lead: str) -> Flujo:
    async with db.obtener_sessionmaker()() as s:
        return (
            await s.execute(select(Flujo).where(Flujo.clave_negocio == lead).options(selectinload(Flujo.pasos)))
        ).scalar_one()


async def _outbox(lead: str) -> list[Outbox]:
    async with db.obtener_sessionmaker()() as s:
        return list(
            (await s.execute(select(Outbox).where(Outbox.clave == lead).order_by(Outbox.creado_en))).scalars()
        )


async def _limpiar(lead: str) -> None:
    async with db.obtener_sessionmaker()() as s, s.begin():
        await s.execute(delete(Outbox).where(Outbox.clave == lead))
        await s.execute(delete(Flujo).where(Flujo.clave_negocio == lead))
        await s.execute(delete(EventoProcesado).where(EventoProcesado.clave.like(f"{lead}%")))


def _nuevo_lead() -> str:
    return f"test-{uuid.uuid4()}"


# ------------------------------------------------------------------ al calificarse el lead


async def test_al_calificarse_el_lead_queda_un_pedido_en_el_outbox(base: Settings) -> None:
    lead = _nuevo_lead()
    try:
        await _calificar(base, lead)

        filas = await _outbox(lead)
        assert len(filas) == 1
        fila = filas[0]
        assert fila.topic == base.kafka_topico_resumen_solicitado
        assert fila.publicado_en is None and fila.abandonado_en is None
        assert fila.intentos == 0
        # el pedido guarda solo a qué lead se refiere: los datos se consultan al publicar
        assert fila.mensaje == {"eventType": "ResumenSolicitado", "leadId": lead, "correlationId": "cid-test"}
        flujo = await _flujo(lead)
        assert fila.flujo_id == flujo.id
        assert flujo.estado == rl.CALIFICADO
    finally:
        await _limpiar(lead)


async def test_con_el_despachador_apagado_no_se_encola_nada(base: Settings) -> None:
    lead = _nuevo_lead()
    MANEJADORES.clear()
    servicio.registrar_manejadores(_settings(orquestador_publica_resumen=False))
    try:
        await _calificar(base, lead)

        assert await _outbox(lead) == []
        assert (await _flujo(lead)).estado == rl.CALIFICADO  # sigue observando, como antes
    finally:
        await _limpiar(lead)


async def test_una_correccion_manual_no_encola_otro_resumen(base: Settings) -> None:
    lead = _nuevo_lead()
    calif = base.kafka_topico_lead_calificado
    try:
        await _calificar(base, lead)
        await MANEJADORES[calif]({"leadId": lead, "origen": "MANUAL"}, registro(calif, datetime.now(timezone.utc)))

        assert len(await _outbox(lead)) == 1
        assert [p.paso for p in (await _flujo(lead)).pasos] == [rl.CALIFICAR_LEAD, rl.CORRECCION_CALIFICACION]
    finally:
        await _limpiar(lead)


async def test_una_calificacion_repetida_no_duplica_el_pedido(base: Settings) -> None:
    lead = _nuevo_lead()
    calif = base.kafka_topico_lead_calificado
    try:
        await _calificar(base, lead)
        # otro mensaje automático (otro offset), con el resumen todavía sin publicar
        await MANEJADORES[calif]({"leadId": lead, "origen": "AUTOMATICA"}, registro(calif, datetime.now(timezone.utc)))

        assert len(await _outbox(lead)) == 1
    finally:
        await _limpiar(lead)


# ------------------------------------------------------------------ despachador


async def test_despacha_el_pedido_con_los_datos_del_lead(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    try:
        await _calificar(base, lead)

        publicados = await despachador.despachar_outbox(base, kafka, ClientesHttp(leads.transporte()))

        assert publicados >= 1
        (topico, clave, sobre), = [p for p in kafka.publicados if p[1] == lead]
        assert topico == base.kafka_topico_resumen_solicitado
        assert sobre["eventType"] == "ResumenSolicitado"
        assert sobre["correlationId"] == "cid-test"
        assert sobre["payload"] == armar_payload_resumen(lead_de(lead))
        # los datos se consultaron a leads, identificándose y con el correlation id del flujo
        pedido = next(p for p in leads.pedidos if p.url.path == f"/leads/{lead}")
        assert pedido.method == "GET"
        assert pedido.headers["x-origen-servicio"] == "orquestador"
        assert pedido.headers["x-correlation-id"] == "cid-test"

        fila, = await _outbox(lead)
        assert fila.publicado_en is not None
        flujo = await _flujo(lead)
        assert flujo.estado == rl.RESUMEN_PENDIENTE
        generar = [p for p in flujo.pasos if p.paso == rl.GENERAR_RESUMEN]
        assert [(p.estado, p.referencia) for p in generar] == [(rl.PENDIENTE, sobre["eventId"])]
    finally:
        await _limpiar(lead)


async def test_el_eco_del_propio_comando_y_el_resumen_cierran_el_flujo(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    try:
        await _calificar(base, lead)
        await despachador.despachar_outbox(base, kafka, ClientesHttp(leads.transporte()))
        sobre = next(p[2] for p in kafka.publicados if p[1] == lead)

        # el orquestador también lee resumen-solicitado: ve su propio comando
        solicitado, generado = base.kafka_topico_resumen_solicitado, base.kafka_topico_resumen_generado
        await MANEJADORES[solicitado](sobre, registro(solicitado, datetime.now(timezone.utc)))
        await MANEJADORES[generado](
            {"eventId": f"{lead}-gen", "payload": {"leadId": lead}}, registro(generado, datetime.now(timezone.utc))
        )

        flujo = await _flujo(lead)
        assert flujo.estado == rl.COMPLETADO
        assert len([p for p in flujo.pasos if p.paso == rl.GENERAR_RESUMEN]) == 1
    finally:
        await _limpiar(lead)


async def test_si_leads_no_responde_el_pedido_se_reintenta_mas_tarde(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    try:
        await _calificar(base, lead)
        ahora = datetime.now(timezone.utc)  # después de calificar: la fila nace con la hora de la base
        leads.caido = True

        await despachador.despachar_outbox(base, kafka, http, ahora=ahora)

        fila, = await _outbox(lead)
        assert fila.publicado_en is None and fila.abandonado_en is None
        assert fila.intentos == 1
        assert "ConnectError" in fila.ultimo_error
        assert fila.proximo_intento_en >= ahora + timedelta(seconds=10)
        assert [p for p in kafka.publicados if p[1] == lead] == []

        # todavía no toca: no se vuelve a intentar antes de la espera
        pedidos = len(leads.pedidos)
        await despachador.despachar_outbox(base, kafka, http, ahora=ahora + timedelta(seconds=5))
        assert len(leads.pedidos) == pedidos

        # leads vuelve y pasó la espera: se publica y el flujo avanza
        leads.caido = False
        await despachador.despachar_outbox(base, kafka, http, ahora=ahora + timedelta(seconds=11))
        assert len([p for p in kafka.publicados if p[1] == lead]) == 1
        fila, = await _outbox(lead)
        assert fila.publicado_en is not None
        assert (await _flujo(lead)).estado == rl.RESUMEN_PENDIENTE
    finally:
        await _limpiar(lead)


async def test_la_espera_entre_intentos_crece(base: Settings) -> None:
    lead = _nuevo_lead()
    leads = LeadsFalso()
    leads.caido = True
    http = ClientesHttp(leads.transporte())
    try:
        await _calificar(base, lead)
        ahora = datetime.now(timezone.utc)  # después de calificar: la fila nace con la hora de la base

        await despachador.despachar_outbox(base, KafkaFalso(), http, ahora=ahora)
        primera, = await _outbox(lead)
        await despachador.despachar_outbox(base, KafkaFalso(), http, ahora=ahora + timedelta(seconds=11))
        segunda, = await _outbox(lead)

        # base 10 s: 10 s tras el primer fallo, 20 s tras el segundo
        assert primera.proximo_intento_en == ahora + timedelta(seconds=10)
        assert segunda.proximo_intento_en == ahora + timedelta(seconds=11 + 20)
    finally:
        await _limpiar(lead)


async def test_al_agotar_los_intentos_se_abandona_y_el_flujo_queda_fallido(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(falla=RuntimeError("broker caído")), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    try:
        await _calificar(base, lead)
        ahora = datetime.now(timezone.utc)  # después de calificar: la fila nace con la hora de la base

        for n in range(base.outbox_max_intentos):
            await despachador.despachar_outbox(base, kafka, http, ahora=ahora + timedelta(seconds=1000 * n))

        fila, = await _outbox(lead)
        assert fila.intentos == base.outbox_max_intentos
        assert fila.abandonado_en is not None and fila.publicado_en is None
        assert "broker caído" in fila.ultimo_error
        flujo = await _flujo(lead)
        assert flujo.estado == rl.FALLIDO
        assert "broker caído" in flujo.ultimo_error

        # un pedido abandonado no se vuelve a intentar solo
        antes = len(leads.pedidos)
        await despachador.despachar_outbox(base, kafka, http, ahora=ahora + timedelta(seconds=10_000))
        assert len(leads.pedidos) == antes
    finally:
        await _limpiar(lead)


async def test_sin_kafka_conectado_no_se_gastan_intentos(base: Settings) -> None:
    lead = _nuevo_lead()
    leads = LeadsFalso()
    try:
        await _calificar(base, lead)

        await despachador.despachar_outbox(base, KafkaFalso(disponible=False), ClientesHttp(leads.transporte()))

        fila, = await _outbox(lead)
        assert fila.intentos == 0 and fila.publicado_en is None
        assert leads.pedidos == []
    finally:
        await _limpiar(lead)


async def test_dos_despachadores_a_la_vez_publican_una_sola_vez(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    try:
        await _calificar(base, lead)

        await asyncio.gather(
            despachador.despachar_outbox(base, kafka, http),
            despachador.despachar_outbox(base, kafka, http),
        )

        assert len([p for p in kafka.publicados if p[1] == lead]) == 1
    finally:
        await _limpiar(lead)


async def test_un_pedido_que_otro_despachador_esta_procesando_se_saltea_sin_esperar(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    try:
        await _calificar(base, lead)
        async with db.obtener_sessionmaker()() as otro, otro.begin():
            # otro despachador tiene tomada la fila (SELECT ... FOR UPDATE sin terminar)
            await otro.execute(select(Outbox).where(Outbox.clave == lead).with_for_update())

            publicados = await asyncio.wait_for(
                despachador.despachar_outbox(base, kafka, ClientesHttp(leads.transporte())), timeout=5
            )

        assert [p for p in kafka.publicados if p[1] == lead] == []
        assert publicados == 0
    finally:
        await _limpiar(lead)


# ------------------------------------------------------------------ reintento manual


async def _post_reintentar(settings: Settings, flujo_id: uuid.UUID) -> httpx.Response:
    app = crear_app(settings)  # sin lifespan: la base ya la abrió la fixture
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://orq") as cliente:
        return await cliente.post(f"/api/orquestador/flujos/{flujo_id}/reintentar")


async def test_reintentar_un_flujo_expirado_encola_un_pedido_nuevo(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    try:
        await _calificar(base, lead)
        await despachador.despachar_outbox(base, kafka, http)
        await servicio.vencer_flujos(ahora=datetime.now(timezone.utc) + timedelta(days=1))
        flujo = await _flujo(lead)
        assert flujo.estado == rl.EXPIRADO

        respuesta = await _post_reintentar(base, flujo.id)

        assert respuesta.status_code == 202
        assert respuesta.json()["estado"] == rl.CALIFICADO
        filas = await _outbox(lead)
        assert len(filas) == 2 and filas[1].publicado_en is None
        # el despachador lo publica de nuevo y el flujo vuelve a esperar al resumen
        await despachador.despachar_outbox(base, kafka, http)
        assert len([p for p in kafka.publicados if p[1] == lead]) == 2
        flujo = await _flujo(lead)
        assert flujo.estado == rl.RESUMEN_PENDIENTE
        assert [(p.intento, p.estado) for p in flujo.pasos if p.paso == rl.GENERAR_RESUMEN] == [
            (1, rl.TIMEOUT),
            (2, rl.PENDIENTE),
        ]
    finally:
        await _limpiar(lead)


async def test_reintentar_con_un_pedido_todavia_pendiente_no_lo_duplica(base: Settings) -> None:
    lead = _nuevo_lead()
    try:
        await _calificar(base, lead)
        flujo = await _flujo(lead)

        respuesta = await _post_reintentar(base, flujo.id)

        assert respuesta.status_code == 202
        assert len(await _outbox(lead)) == 1
    finally:
        await _limpiar(lead)


async def test_reintentar_un_flujo_completado_da_409(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    try:
        await _calificar(base, lead)
        await despachador.despachar_outbox(base, kafka, ClientesHttp(leads.transporte()))
        generado = base.kafka_topico_resumen_generado
        await MANEJADORES[generado](
            {"eventId": f"{lead}-gen", "payload": {"leadId": lead}}, registro(generado, datetime.now(timezone.utc))
        )
        flujo = await _flujo(lead)
        assert flujo.estado == rl.COMPLETADO

        respuesta = await _post_reintentar(base, flujo.id)

        assert respuesta.status_code == 409
        assert respuesta.json()["codigo"] == "FLUJO_COMPLETADO"
    finally:
        await _limpiar(lead)


async def test_reintentar_un_flujo_sin_calificar_da_409(base: Settings) -> None:
    lead = _nuevo_lead()
    pre = base.kafka_topico_preformulario_completado
    try:
        await MANEJADORES[pre](
            {"eventId": f"{lead}-pre", "payload": {"leadId": lead}}, registro(pre, datetime.now(timezone.utc))
        )
        flujo = await _flujo(lead)

        respuesta = await _post_reintentar(base, flujo.id)

        assert respuesta.status_code == 409
        assert respuesta.json()["codigo"] == "FLUJO_SIN_CALIFICAR"
        assert await _outbox(lead) == []
    finally:
        await _limpiar(lead)


async def test_reintentar_con_el_despachador_apagado_da_409(base: Settings) -> None:
    lead = _nuevo_lead()
    try:
        await _calificar(base, lead)
        flujo = await _flujo(lead)

        respuesta = await _post_reintentar(_settings(orquestador_publica_resumen=False), flujo.id)

        assert respuesta.status_code == 409
        assert respuesta.json()["codigo"] == "DESPACHADOR_APAGADO"
    finally:
        await _limpiar(lead)


async def test_reintentar_un_flujo_inexistente_da_404(base: Settings) -> None:
    respuesta = await _post_reintentar(base, uuid.uuid4())

    assert respuesta.status_code == 404
    assert respuesta.json()["codigo"] == "FLUJO_NO_ENCONTRADO"
