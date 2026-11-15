"""Tanda A: el orquestador pide el resumen (lógica pura, sin base ni Kafka)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.config import Settings
from app.db.models import Flujo
from app.flujos import registro_lead as rl
from app.flujos.resumen import armar_payload_resumen

T0 = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)


def t(segundos: float) -> datetime:
    return T0 + timedelta(seconds=segundos)


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None, flujo_timeout_calificacion_segundos=120, flujo_timeout_resumen_segundos=300)


@pytest.fixture
def flujo() -> Flujo:
    return Flujo(tipo=rl.TIPO, clave_negocio="L1", estado=rl.CALIFICACION_PENDIENTE, intentos=0, pasos=[])


def _pasos(flujo: Flujo, nombre: str):
    return [p for p in flujo.pasos if p.paso == nombre]


# ------------------------------------------------- cuándo hay que pedir el resumen


def test_la_primera_calificacion_automatica_pide_el_resumen(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)

    assert rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings) is True


def test_una_correccion_manual_no_pide_otro_resumen(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)

    assert rl.al_lead_calificado(flujo, t(60), "e3", "MANUAL", settings) is False


def test_una_calificacion_con_el_resumen_ya_pedido_no_lo_pide_de_nuevo(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)

    assert rl.al_lead_calificado(flujo, t(3), "e4", "AUTOMATICA", settings) is False


def test_una_calificacion_manual_sin_calificacion_previa_si_pide_el_resumen(
    flujo: Flujo, settings: Settings
) -> None:
    # Es la primera calificación que ve el flujo: no es una corrección de nada.
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)

    assert rl.al_lead_calificado(flujo, t(1), "e2", "MANUAL", settings) is True


# ------------------------------------------------- reintentos del paso


def test_un_pedido_nuevo_despues_de_un_timeout_abre_otro_intento(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    rl.vencer(flujo, t(400))

    rl.al_resumen_solicitado(flujo, t(500), "e4", settings)

    intentos = _pasos(flujo, rl.GENERAR_RESUMEN)
    assert [(p.intento, p.estado) for p in intentos] == [(1, rl.TIMEOUT), (2, rl.PENDIENTE)]
    assert flujo.estado == rl.RESUMEN_PENDIENTE
    # la latencia final se mide desde el último intento, no desde el primero
    rl.al_resumen_generado(flujo, t(510), "e5", settings)
    assert intentos[1].latencia_ms == 10_000


def test_el_pedido_repetido_con_el_paso_pendiente_no_abre_otro(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)

    rl.al_resumen_solicitado(flujo, t(3), "e4", settings)  # el eco del propio comando

    assert len(_pasos(flujo, rl.GENERAR_RESUMEN)) == 1


def test_preparar_reintento_deja_el_flujo_esperando_el_pedido(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    rl.vencer(flujo, t(400))
    assert flujo.estado == rl.EXPIRADO

    rl.preparar_reintento(flujo, t(500), settings)

    assert flujo.estado == rl.CALIFICADO
    assert flujo.vence_en == t(800)
    assert flujo.ultimo_error is None


@pytest.mark.parametrize("estado", [rl.EXPIRADO, rl.FALLIDO, rl.CALIFICADO, rl.RESUMEN_PENDIENTE])
def test_se_puede_reintentar_un_flujo_ya_calificado_que_no_termino(
    flujo: Flujo, settings: Settings, estado: str
) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    flujo.estado = estado

    rl.preparar_reintento(flujo, t(10), settings)  # no levanta


def test_no_se_reintenta_un_flujo_completado(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    rl.al_resumen_generado(flujo, t(3), "e4", settings)

    with pytest.raises(rl.ReintentoNoPermitido) as error:
        rl.preparar_reintento(flujo, t(10), settings)
    assert error.value.codigo == "FLUJO_COMPLETADO"


def test_no_se_reintenta_un_flujo_que_todavia_no_fue_calificado(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)

    with pytest.raises(rl.ReintentoNoPermitido) as error:
        rl.preparar_reintento(flujo, t(10), settings)
    assert error.value.codigo == "FLUJO_SIN_CALIFICAR"


def test_un_pedido_que_no_se_pudo_publicar_deja_el_flujo_fallido(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)

    rl.al_resumen_no_publicado(flujo, "Kafka no disponible")

    assert flujo.estado == rl.FALLIDO
    assert flujo.vence_en is None
    assert "Kafka no disponible" in flujo.ultimo_error


def test_un_pedido_abandonado_no_pisa_un_flujo_que_ya_avanzo(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)  # llegó por otro lado (ej. leads)

    rl.al_resumen_no_publicado(flujo, "Kafka no disponible")

    assert flujo.estado == rl.RESUMEN_PENDIENTE


# ------------------------------------------------- payload del comando


LEAD = {
    "id": "11111111-1111-1111-1111-111111111111",
    "cliente_id": "22222222-2222-2222-2222-222222222222",
    "vehiculo_id": "33333333-3333-3333-3333-333333333333",
    "anticipo": 1500000.0,
    "solicita_financiacion": True,
    "permuta_tiene": True,
    "permuta_descripcion": "Fiat Palio 2012",
    "permuta_valor_declarado": 4000000.0,
    "provincia": "Córdoba",
    "localidad": "Río Cuarto",
    # datos que NO viajan en el comando
    "estado": "CALIFICADO",
    "nivel": "POTABLE",
    "puntaje": 70,
    "resumen": None,
}


def test_el_payload_respeta_el_contrato_que_publicaba_leads() -> None:
    assert armar_payload_resumen(LEAD) == {
        "leadId": "11111111-1111-1111-1111-111111111111",
        "clienteId": "22222222-2222-2222-2222-222222222222",
        "vehiculoId": "33333333-3333-3333-3333-333333333333",
        "anticipo": 1500000.0,
        "solicitaFinanciacion": True,
        "permuta": {
            "tienePermuta": True,
            "descripcion": "Fiat Palio 2012",
            "valorDeclarado": 4000000.0,
        },
        "ubicacion": {"provincia": "Córdoba", "localidad": "Río Cuarto"},
    }


def test_el_payload_tolera_permuta_y_localidad_vacias() -> None:
    lead = {**LEAD, "permuta_tiene": False, "permuta_descripcion": None, "permuta_valor_declarado": None, "localidad": None}

    payload = armar_payload_resumen(lead)

    assert payload["permuta"] == {"tienePermuta": False, "descripcion": None, "valorDeclarado": None}
    assert payload["ubicacion"] == {"provincia": "Córdoba", "localidad": None}


def test_el_payload_falla_si_leads_no_devuelve_un_dato_obligatorio() -> None:
    lead = {k: v for k, v in LEAD.items() if k != "cliente_id"}

    with pytest.raises(ValueError, match="cliente_id"):
        armar_payload_resumen(lead)
