"""Tanda B: orquestación con tres tópicos (crm.eventos → orquestador → crm.comandos), contra Postgres real.

Contrato: docs/08-contrato-tres-topicos.md. Se saltean si no hay base: definir TEST_DATABASE_URL o tener
levantado `docker compose up -d postgres` con las migraciones aplicadas (`alembic upgrade head`).
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
_offsets = count(random.randint(10**9, 10**12))


@pytest.fixture(scope="session")
def event_loop_policy():
    # psycopg async no funciona con el loop por defecto de Windows.
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.DefaultEventLoopPolicy()


@dataclass
class Registro:
    """Imita un ConsumerRecord de aiokafka."""

    topic: str
    partition: int
    offset: int
    timestamp: int


def _settings(**cambios: Any) -> Settings:
    valores: dict[str, Any] = dict(
        _env_file=None,
        database_url=URL,
        kafka_enabled=False,
        auth_habilitada=False,
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
            tiene_flujo_id = (
                await s.execute(
                    text(
                        "select 1 from information_schema.columns where table_schema = 'orquestador' "
                        "and table_name = 'outbox' and column_name = 'flujo_id'"
                    )
                )
            ).scalar()
        if tiene_flujo_id is None:
            pytest.skip("Faltan las migraciones (alembic upgrade head).")
        listo = True
        MANEJADORES.clear()
        servicio.registrar_manejadores(settings)
        yield settings
    finally:
        MANEJADORES.clear()
        if listo:
            async with db.obtener_sessionmaker()() as s, s.begin():
                await s.execute(delete(Outbox).where(Outbox.clave.like("test-%")))
                await s.execute(delete(Flujo).where(Flujo.clave_negocio.like("test-%")))
                await s.execute(delete(EventoProcesado).where(EventoProcesado.clave.like("test-%")))
        await db.cerrar_db()


# ------------------------------------------------------------------ dobles de prueba


class KafkaFalso:
    def __init__(self, disponible: bool = True, falla: Optional[Exception] = None) -> None:
        self.disponible = disponible
        self.falla = falla
        self.publicados: list[tuple[str, str, dict[str, Any]]] = []

    async def publicar(self, topico, clave, event_type, payload, correlation_id=None, destino=None):
        if self.falla is not None:
            raise self.falla
        sobre = armar_sobre(event_type, payload, correlation_id, destino=destino)
        self.publicados.append((topico, clave, sobre))
        return sobre

    def del_lead(self, lead: str) -> list[dict[str, Any]]:
        return [sobre for _, clave, sobre in self.publicados if clave == lead]


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
            return httpx.Response(200, json=lead_de(request.url.path.rsplit("/", 1)[-1]))

        return httpx.MockTransport(manejar)


# ------------------------------------------------------------------ mensajes


def preformulario(lead: str) -> dict[str, Any]:
    return {
        "leadId": lead,
        "clienteId": "22222222-2222-2222-2222-222222222222",
        "vehiculoId": "33333333-3333-3333-3333-333333333333",
        "montoAnticipo": 1500000.0,
        "montoPermuta": 0,
        "permutaNoComputable": False,
        "solicitaFinanciacion": True,
        "ubicacion": "Córdoba",
    }


def calificacion(lead: str, origen: str = "AUTOMATICA", nivel: str = "POTABLE") -> dict[str, Any]:
    return {
        "leadId": lead,
        "clienteId": "22222222-2222-2222-2222-222222222222",
        "puntos": 70,
        "nivel": nivel,
        "origen": origen,
        "fecha": "2026-11-15T12:00:01",
        "observacion": None,
        "motivos": ["[COBERTURA]: anticipo alto (+60 pts)"],
    }


def sobre(event_type: str, payload: dict[str, Any], cid: str = "cid-test") -> dict[str, Any]:
    mensaje = armar_sobre(event_type, payload, cid)
    mensaje["eventId"] = f"{payload['leadId']}-{uuid.uuid4()}"  # se limpia por el prefijo del lead
    return mensaje


async def evento(settings: Settings, mensaje: dict[str, Any], momento: Optional[datetime] = None) -> None:
    await _entregar(settings.kafka_topico_eventos, mensaje, momento)


async def a_la_dlt(settings: Settings, mensaje: dict[str, Any]) -> None:
    await _entregar(settings.kafka_topico_dlt, mensaje, None)


async def _entregar(topico: str, mensaje: dict[str, Any], momento: Optional[datetime]) -> None:
    momento = momento or datetime.now(timezone.utc)
    reg = Registro(topico, 0, next(_offsets), int(momento.timestamp() * 1000))
    await MANEJADORES[topico](mensaje, reg)


async def _registrar_lead(settings: Settings, lead: str, t0: Optional[datetime] = None) -> None:
    t0 = t0 or datetime.now(timezone.utc)
    await evento(settings, sobre("PreformularioCompletado", preformulario(lead)), t0)


async def _calificar(settings: Settings, lead: str, origen: str = "AUTOMATICA") -> None:
    t0 = datetime.now(timezone.utc)
    await _registrar_lead(settings, lead, t0)
    await evento(settings, sobre("LeadCalificado", calificacion(lead, origen)), t0 + timedelta(seconds=1))


async def _flujo(lead: str) -> Flujo:
    async with db.obtener_sessionmaker()() as s:
        return (
            await s.execute(select(Flujo).where(Flujo.clave_negocio == lead).options(selectinload(Flujo.pasos)))
        ).scalar_one()


async def _hay_flujo(lead: str) -> bool:
    async with db.obtener_sessionmaker()() as s:
        return (await s.execute(select(Flujo.id).where(Flujo.clave_negocio == lead))).first() is not None


async def _outbox(lead: str) -> list[Outbox]:
    async with db.obtener_sessionmaker()() as s:
        return list(
            (
                await s.execute(select(Outbox).where(Outbox.clave == lead).order_by(Outbox.creado_en, Outbox.id))
            ).scalars()
        )


async def _pendientes(lead: str) -> dict[str, Outbox]:
    return {f.mensaje["eventType"]: f for f in await _outbox(lead) if f.publicado_en is None}


async def _despachar(settings: Settings, kafka: KafkaFalso, http: ClientesHttp, ahora: Optional[datetime] = None) -> int:
    return await despachador.despachar_outbox(settings, kafka, http, ahora=ahora)


def _nuevo_lead() -> str:
    return f"test-{uuid.uuid4()}"


# ------------------------------------------------------------------ qué ordena cada evento


async def test_el_preformulario_ordena_calificar_el_lead(base: Settings) -> None:
    lead = _nuevo_lead()
    await _registrar_lead(base, lead)

    flujo = await _flujo(lead)
    assert flujo.estado == rl.CALIFICACION_PENDIENTE
    assert flujo.correlation_id == "cid-test"
    fila, = await _outbox(lead)
    assert fila.topic == base.kafka_topico_comandos
    assert fila.mensaje["eventType"] == "CalificarLead"
    assert fila.mensaje["destino"] == "calificacion"
    # reenvía el payload del evento tal cual (mientras está pendiente)
    assert fila.mensaje["payload"] == preformulario(lead)


async def test_la_primera_calificacion_ordena_registrar_actualizar_y_resumir(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)

    pendientes = await _pendientes(lead)
    assert set(pendientes) == {"CalificarLead", "RegistrarCalificacion", "ActualizarCalificacion", "GenerarResumen"}
    assert pendientes["RegistrarCalificacion"].mensaje["destino"] == "clientes"
    assert pendientes["ActualizarCalificacion"].mensaje["destino"] == "leads"
    assert pendientes["GenerarResumen"].mensaje["destino"] == "ia-resumen"
    assert pendientes["RegistrarCalificacion"].mensaje["payload"] == calificacion(lead)
    assert pendientes["ActualizarCalificacion"].mensaje["payload"] == calificacion(lead)
    # GenerarResumen no guarda datos: los consulta a leads al publicar
    assert "payload" not in pendientes["GenerarResumen"].mensaje
    assert (await _flujo(lead)).estado == rl.CALIFICADO


async def test_una_correccion_manual_se_reenvia_pero_no_regenera_el_resumen(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)
    await evento(base, sobre("LeadCalificado", calificacion(lead, "MANUAL", nivel="MUY_POTABLE")))

    tipos = [f.mensaje["eventType"] for f in await _outbox(lead)]
    assert tipos.count("RegistrarCalificacion") == 2
    assert tipos.count("ActualizarCalificacion") == 2
    assert tipos.count("GenerarResumen") == 1
    manual = [f for f in await _outbox(lead) if f.mensaje["eventType"] == "RegistrarCalificacion"][-1]
    assert manual.mensaje["payload"]["nivel"] == "MUY_POTABLE"


async def test_una_correccion_manual_con_el_resumen_ya_publicado_no_lo_pide_de_nuevo(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    await _calificar(base, lead)
    await _despachar(base, kafka, http)  # GenerarResumen ya salió: no hay uno pendiente que oculte el caso

    await evento(base, sobre("LeadCalificado", calificacion(lead, "MANUAL", nivel="MUY_POTABLE")))
    await _despachar(base, kafka, http)

    tipos = [s["eventType"] for s in kafka.del_lead(lead)]
    assert tipos.count("GenerarResumen") == 1
    assert tipos.count("RegistrarCalificacion") == 2


async def test_un_segundo_preformulario_con_la_calificacion_ya_ordenada_no_la_ordena_de_nuevo(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka = KafkaFalso()
    await _registrar_lead(base, lead)
    await _despachar(base, kafka, ClientesHttp(LeadsFalso().transporte()))  # CalificarLead ya salió

    await _registrar_lead(base, lead)  # otro evento (otro eventId) del mismo lead
    await _despachar(base, kafka, ClientesHttp(LeadsFalso().transporte()))

    assert [s["eventType"] for s in kafka.del_lead(lead)] == ["CalificarLead"]


async def test_un_resumen_vacio_completa_el_flujo_sin_ordenar_guardarlo(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)

    await evento(base, sobre("ResumenGenerado", {"leadId": lead, "resumen": ""}))

    assert (await _flujo(lead)).estado == rl.COMPLETADO
    assert "GuardarResumen" not in await _pendientes(lead)


async def test_el_resumen_generado_ordena_guardarlo_y_completa_el_flujo(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    await _calificar(base, lead)
    await _despachar(base, kafka, ClientesHttp(leads.transporte()))

    await evento(base, sobre("ResumenGenerado", {"leadId": lead, "resumen": "Cliente interesado en el Corolla."}))

    flujo = await _flujo(lead)
    assert flujo.estado == rl.COMPLETADO
    guardar = (await _pendientes(lead))["GuardarResumen"]
    assert guardar.mensaje["destino"] == "leads"
    assert guardar.mensaje["payload"] == {"leadId": lead, "resumen": "Cliente interesado en el Corolla."}


async def test_un_evento_que_el_orquestador_no_usa_se_ignora(base: Settings) -> None:
    lead = _nuevo_lead()
    await evento(base, sobre("ClienteActualizado", {"leadId": lead, "clienteId": "c1"}))

    assert not await _hay_flujo(lead)


async def test_un_mensaje_sin_sobre_se_ignora(base: Settings) -> None:
    lead = _nuevo_lead()
    await evento(base, {"leadId": lead, "nivel": "POTABLE"})  # el formato viejo de calificacion

    assert not await _hay_flujo(lead)


async def test_un_evento_duplicado_se_procesa_una_vez(base: Settings) -> None:
    lead = _nuevo_lead()
    mensaje = sobre("PreformularioCompletado", preformulario(lead))

    await evento(base, mensaje)
    await evento(base, mensaje)

    assert len(await _outbox(lead)) == 1
    assert (await _flujo(lead)).version == 2  # una sola actualización después del alta


async def test_eventos_simultaneos_del_mismo_lead_no_duplican_el_flujo(base: Settings) -> None:
    lead = _nuevo_lead()
    t0 = datetime.now(timezone.utc)

    await asyncio.gather(
        evento(base, sobre("PreformularioCompletado", preformulario(lead)), t0),
        evento(base, sobre("LeadCalificado", calificacion(lead)), t0 + timedelta(seconds=1)),
    )

    async with db.obtener_sessionmaker()() as s:
        cantidad = (
            await s.execute(text("select count(*) from orquestador.flujo where clave_negocio = :l"), {"l": lead})
        ).scalar()
    assert cantidad == 1
    assert (await _flujo(lead)).estado == rl.CALIFICADO


async def test_vencer_flujos_marca_los_trabados(base: Settings) -> None:
    lead = _nuevo_lead()
    t0 = datetime.now(timezone.utc)
    await _registrar_lead(base, lead, t0)

    await servicio.vencer_flujos(ahora=t0 + timedelta(seconds=base.flujo_timeout_calificacion_segundos + 1))

    flujo = await _flujo(lead)
    assert flujo.estado == rl.EXPIRADO
    assert flujo.pasos[0].estado == rl.TIMEOUT


# ------------------------------------------------------------------ crm.dlt


async def test_calificar_lead_en_la_dlt_deja_el_flujo_fallido(base: Settings) -> None:
    lead = _nuevo_lead()
    await _registrar_lead(base, lead)

    comando = armar_sobre("CalificarLead", preformulario(lead), "cid-test", destino="calificacion")
    comando["eventId"] = f"{lead}-dlt"
    await a_la_dlt(base, comando)

    flujo = await _flujo(lead)
    assert flujo.estado == rl.FALLIDO
    assert flujo.pasos[0].estado == rl.ERROR


async def test_un_reenvio_en_la_dlt_queda_como_paso_con_error_sin_cambiar_el_estado(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)

    comando = armar_sobre("RegistrarCalificacion", calificacion(lead), "cid-test", destino="clientes")
    comando["eventId"] = f"{lead}-dlt"
    await a_la_dlt(base, comando)

    flujo = await _flujo(lead)
    assert flujo.estado == rl.CALIFICADO
    fallido = [p for p in flujo.pasos if p.paso == "REGISTRAR_CALIFICACION"]
    assert [(p.servicio, p.estado) for p in fallido] == [("clientes", rl.ERROR)]


# ------------------------------------------------------------------ despachador


async def test_despacha_calificar_lead_con_destino_y_borra_el_payload_al_publicar(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    await _registrar_lead(base, lead)

    await _despachar(base, kafka, ClientesHttp(leads.transporte()))

    (topico, _, comando), = [p for p in kafka.publicados if p[1] == lead]
    assert topico == base.kafka_topico_comandos
    assert comando["eventType"] == "CalificarLead"
    assert comando["destino"] == "calificacion"
    assert comando["correlationId"] == "cid-test"
    assert comando["payload"] == preformulario(lead)
    fila, = await _outbox(lead)
    assert fila.publicado_en is not None
    assert "payload" not in fila.mensaje  # ya publicado: no se guardan datos de negocio


async def test_despacha_generar_resumen_con_los_datos_del_lead(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    await _calificar(base, lead)

    await _despachar(base, kafka, ClientesHttp(leads.transporte()))

    resumen, = [s for s in kafka.del_lead(lead) if s["eventType"] == "GenerarResumen"]
    assert resumen["destino"] == "ia-resumen"
    assert resumen["payload"] == armar_payload_resumen(lead_de(lead))
    pedido = next(p for p in leads.pedidos if p.url.path == f"/leads/{lead}")
    assert pedido.headers["x-origen-servicio"] == "orquestador"
    assert pedido.headers["x-correlation-id"] == "cid-test"
    flujo = await _flujo(lead)
    assert flujo.estado == rl.RESUMEN_PENDIENTE
    assert [(p.estado, p.referencia) for p in flujo.pasos if p.paso == rl.GENERAR_RESUMEN] == [
        (rl.PENDIENTE, resumen["eventId"])
    ]


async def test_flujo_completo_por_kafka(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    t0 = datetime.now(timezone.utc)

    await _registrar_lead(base, lead, t0)
    await _despachar(base, kafka, http)
    await evento(base, sobre("LeadCalificado", calificacion(lead)), t0 + timedelta(seconds=1))
    await _despachar(base, kafka, http)
    await evento(base, sobre("ResumenGenerado", {"leadId": lead, "resumen": "ok"}), datetime.now(timezone.utc))
    await _despachar(base, kafka, http)

    assert [s["eventType"] for s in kafka.del_lead(lead)] == [
        "CalificarLead", "RegistrarCalificacion", "ActualizarCalificacion", "GenerarResumen", "GuardarResumen",
    ]
    assert all(s["correlationId"] == "cid-test" for s in kafka.del_lead(lead))
    flujo = await _flujo(lead)
    assert flujo.estado == rl.COMPLETADO
    assert {p.paso: p.estado for p in flujo.pasos} == {rl.CALIFICAR_LEAD: rl.OK, rl.GENERAR_RESUMEN: rl.OK}
    assert {p.paso: p.latencia_ms for p in flujo.pasos}[rl.CALIFICAR_LEAD] == 1000
    assert all("payload" not in f.mensaje for f in await _outbox(lead))


async def test_los_comandos_de_un_lead_salen_en_orden(base: Settings) -> None:
    lead, otro = _nuevo_lead(), _nuevo_lead()
    leads = LeadsFalso()
    leads.caido = True
    http = ClientesHttp(leads.transporte())
    await _calificar(base, lead)
    await _registrar_lead(base, otro)
    ahora = datetime.now(timezone.utc)
    kafka = KafkaFalso()
    # el CalificarLead del lead se publica; después GenerarResumen falla (leads caído) y queda esperando
    await _despachar(base, kafka, http, ahora=ahora)

    await evento(base, sobre("ResumenGenerado", {"leadId": lead, "resumen": "ok"}))
    await _despachar(base, kafka, http, ahora=datetime.now(timezone.utc))

    # GuardarResumen es posterior a GenerarResumen, que sigue pendiente: no se adelanta
    assert "GuardarResumen" not in [s["eventType"] for s in kafka.del_lead(lead)]
    # el otro lead no queda trabado por el primero
    assert [s["eventType"] for s in kafka.del_lead(otro)] == ["CalificarLead"]

    leads.caido = False
    await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=11))
    tipos = [s["eventType"] for s in kafka.del_lead(lead)]
    assert tipos.index("GenerarResumen") < tipos.index("GuardarResumen")


async def test_si_leads_no_responde_el_resumen_se_reintenta_mas_tarde(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    await _calificar(base, lead)
    ahora = datetime.now(timezone.utc)  # después de calificar: la fila nace con la hora de la base
    leads.caido = True

    await _despachar(base, kafka, http, ahora=ahora)

    fila = (await _pendientes(lead))["GenerarResumen"]
    assert fila.intentos == 1
    assert "ConnectError" in fila.ultimo_error
    assert fila.proximo_intento_en >= ahora + timedelta(seconds=10)

    pedidos = len(leads.pedidos)
    await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=5))
    assert len(leads.pedidos) == pedidos  # todavía no le toca

    leads.caido = False
    await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=11))
    assert [s["eventType"] for s in kafka.del_lead(lead)].count("GenerarResumen") == 1
    assert (await _flujo(lead)).estado == rl.RESUMEN_PENDIENTE


async def test_la_espera_entre_intentos_crece(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka = KafkaFalso(falla=RuntimeError("broker caído"))
    http = ClientesHttp(LeadsFalso().transporte())
    await _registrar_lead(base, lead)
    ahora = datetime.now(timezone.utc)

    await _despachar(base, kafka, http, ahora=ahora)
    primera, = await _outbox(lead)
    await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=11))
    segunda, = await _outbox(lead)

    # base 10 s: 10 s tras el primer fallo, 20 s tras el segundo
    assert primera.proximo_intento_en == ahora + timedelta(seconds=10)
    assert segunda.proximo_intento_en == ahora + timedelta(seconds=11 + 20)


async def test_al_agotar_los_intentos_se_abandona_y_el_flujo_queda_fallido(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka = KafkaFalso(falla=RuntimeError("broker caído"))
    http = ClientesHttp(LeadsFalso().transporte())
    await _registrar_lead(base, lead)
    ahora = datetime.now(timezone.utc)

    for n in range(base.outbox_max_intentos):
        await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=1000 * n))

    fila, = await _outbox(lead)
    assert fila.intentos == base.outbox_max_intentos
    assert fila.abandonado_en is not None and fila.publicado_en is None
    assert "payload" not in fila.mensaje  # abandonado: tampoco se guardan los datos
    flujo = await _flujo(lead)
    assert flujo.estado == rl.FALLIDO
    assert "broker caído" in flujo.ultimo_error

    intentos = fila.intentos
    await _despachar(base, kafka, http, ahora=ahora + timedelta(seconds=10_000))
    assert (await _outbox(lead))[0].intentos == intentos  # no se vuelve a intentar solo


async def test_sin_kafka_conectado_no_se_gastan_intentos(base: Settings) -> None:
    lead = _nuevo_lead()
    await _registrar_lead(base, lead)

    await _despachar(base, KafkaFalso(disponible=False), ClientesHttp(LeadsFalso().transporte()))

    fila, = await _outbox(lead)
    assert fila.intentos == 0 and fila.publicado_en is None


async def test_un_pedido_que_otro_despachador_esta_procesando_se_saltea_sin_esperar(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka = KafkaFalso()
    await _registrar_lead(base, lead)
    async with db.obtener_sessionmaker()() as otro, otro.begin():
        # otro despachador tiene tomada la fila (SELECT ... FOR UPDATE sin terminar)
        await otro.execute(select(Outbox).where(Outbox.clave == lead).with_for_update())

        await asyncio.wait_for(_despachar(base, kafka, ClientesHttp(LeadsFalso().transporte())), timeout=5)

    assert kafka.del_lead(lead) == []


async def test_dos_despachadores_a_la_vez_publican_una_sola_vez(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka = KafkaFalso()
    http = ClientesHttp(LeadsFalso().transporte())
    await _registrar_lead(base, lead)

    await asyncio.gather(_despachar(base, kafka, http), _despachar(base, kafka, http))

    assert len(kafka.del_lead(lead)) == 1


# ------------------------------------------------------------------ reintento manual


async def _post_reintentar(settings: Settings, flujo_id: uuid.UUID) -> httpx.Response:
    app = crear_app(settings)  # sin lifespan: la base ya la abrió la fixture
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://orq") as cliente:
        return await cliente.post(f"/api/orquestador/flujos/{flujo_id}/reintentar")


async def test_reintentar_un_flujo_expirado_ordena_de_nuevo_el_resumen(base: Settings) -> None:
    lead = _nuevo_lead()
    kafka, leads = KafkaFalso(), LeadsFalso()
    http = ClientesHttp(leads.transporte())
    await _calificar(base, lead)
    await _despachar(base, kafka, http)
    await servicio.vencer_flujos(ahora=datetime.now(timezone.utc) + timedelta(days=1))
    flujo = await _flujo(lead)
    assert flujo.estado == rl.EXPIRADO

    respuesta = await _post_reintentar(base, flujo.id)

    assert respuesta.status_code == 202
    assert respuesta.json()["estado"] == rl.CALIFICADO
    await _despachar(base, kafka, http)
    assert [s["eventType"] for s in kafka.del_lead(lead)].count("GenerarResumen") == 2
    flujo = await _flujo(lead)
    assert flujo.estado == rl.RESUMEN_PENDIENTE
    assert [(p.intento, p.estado) for p in flujo.pasos if p.paso == rl.GENERAR_RESUMEN] == [
        (1, rl.TIMEOUT),
        (2, rl.PENDIENTE),
    ]


async def test_reintentar_con_el_resumen_todavia_pendiente_no_lo_duplica(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)

    respuesta = await _post_reintentar(base, (await _flujo(lead)).id)

    assert respuesta.status_code == 202
    assert [f.mensaje["eventType"] for f in await _outbox(lead)].count("GenerarResumen") == 1


async def test_reintentar_un_flujo_completado_da_409(base: Settings) -> None:
    lead = _nuevo_lead()
    await _calificar(base, lead)
    await _despachar(base, KafkaFalso(), ClientesHttp(LeadsFalso().transporte()))
    await evento(base, sobre("ResumenGenerado", {"leadId": lead, "resumen": "ok"}))

    respuesta = await _post_reintentar(base, (await _flujo(lead)).id)

    assert respuesta.status_code == 409
    assert respuesta.json()["codigo"] == "FLUJO_COMPLETADO"


async def test_reintentar_un_flujo_sin_calificar_da_409(base: Settings) -> None:
    lead = _nuevo_lead()
    await _registrar_lead(base, lead)

    respuesta = await _post_reintentar(base, (await _flujo(lead)).id)

    assert respuesta.status_code == 409
    assert respuesta.json()["codigo"] == "FLUJO_SIN_CALIFICAR"
    assert [f.mensaje["eventType"] for f in await _outbox(lead)] == ["CalificarLead"]


async def test_reintentar_un_flujo_inexistente_da_404(base: Settings) -> None:
    respuesta = await _post_reintentar(base, uuid.uuid4())

    assert respuesta.status_code == 404
    assert respuesta.json()["codigo"] == "FLUJO_NO_ENCONTRADO"
