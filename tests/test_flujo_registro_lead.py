"""Máquina de estados del flujo REGISTRO_LEAD (sin base de datos)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.config import Settings
from app.db.models import Flujo
from app.flujos import registro_lead as rl

T0 = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)


def t(segundos: float) -> datetime:
    return T0 + timedelta(seconds=segundos)


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None, flujo_timeout_calificacion_segundos=120, flujo_timeout_resumen_segundos=300)


@pytest.fixture
def flujo() -> Flujo:
    return Flujo(tipo=rl.TIPO, clave_negocio="L1", estado=rl.CALIFICACION_PENDIENTE, intentos=0, pasos=[])


def _paso(flujo: Flujo, nombre: str):
    return next(p for p in flujo.pasos if p.paso == nombre)


def test_camino_feliz_con_latencias(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    assert flujo.estado == rl.CALIFICACION_PENDIENTE
    assert flujo.vence_en == t(120)

    rl.al_lead_calificado(flujo, t(1.5), "e2", "AUTOMATICA", settings)
    assert flujo.estado == rl.CALIFICADO
    assert _paso(flujo, rl.CALIFICAR_LEAD).latencia_ms == 1500

    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    assert flujo.estado == rl.RESUMEN_PENDIENTE
    assert flujo.vence_en == t(302)

    rl.al_resumen_generado(flujo, t(10), "e4", settings)
    assert flujo.estado == rl.COMPLETADO
    assert flujo.vence_en is None
    assert flujo.finalizado_en is not None
    assert _paso(flujo, rl.GENERAR_RESUMEN).latencia_ms == 8000


def test_evento_atrasado_no_hace_retroceder(flujo: Flujo, settings: Settings) -> None:
    # lead-calificado llega antes que preformulario-completado
    rl.al_lead_calificado(flujo, t(2), "e2", "AUTOMATICA", settings)
    rl.al_preformulario_completado(flujo, t(1), "e1", settings)

    assert flujo.estado == rl.CALIFICADO
    assert [p.paso for p in flujo.pasos] == [rl.CALIFICAR_LEAD]  # no se abre otro paso
    assert _paso(flujo, rl.CALIFICAR_LEAD).estado == rl.OK


def test_vence_y_se_recupera_si_el_evento_llega_tarde(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.vencer(flujo, t(121))

    assert flujo.estado == rl.EXPIRADO
    assert "calificacion" in flujo.ultimo_error
    assert _paso(flujo, rl.CALIFICAR_LEAD).estado == rl.TIMEOUT

    rl.al_lead_calificado(flujo, t(200), "e2", "AUTOMATICA", settings)

    assert flujo.estado == rl.CALIFICADO
    assert flujo.ultimo_error is None
    assert _paso(flujo, rl.CALIFICAR_LEAD).estado == rl.OK
    assert _paso(flujo, rl.CALIFICAR_LEAD).latencia_ms == 200_000


def test_vencido_esperando_el_resumen(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    rl.vencer(flujo, t(400))

    assert flujo.estado == rl.EXPIRADO
    assert "ia-resumen" in flujo.ultimo_error
    assert _paso(flujo, rl.GENERAR_RESUMEN).estado == rl.TIMEOUT
    # Un preformulario repetido que llega tarde no lo hace retroceder.
    rl.al_preformulario_completado(flujo, t(401), "e1b", settings)
    assert flujo.estado == rl.EXPIRADO


def test_correccion_manual_no_cambia_el_estado(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_resumen_solicitado(flujo, t(2), "e3", settings)
    rl.al_resumen_generado(flujo, t(3), "e4", settings)

    rl.al_lead_calificado(flujo, t(100), "e5", "MANUAL", settings)

    assert flujo.estado == rl.COMPLETADO
    assert flujo.pasos[-1].paso == rl.CORRECCION_CALIFICACION


def test_dlt_marca_el_flujo_como_fallido(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_preformulario_en_dlt(flujo, t(5), "dlt1", settings)

    assert flujo.estado == rl.FALLIDO
    assert flujo.vence_en is None
    assert _paso(flujo, rl.CALIFICAR_LEAD).estado == rl.ERROR


def test_respuesta_repetida_no_pisa_el_paso_cerrado(flujo: Flujo, settings: Settings) -> None:
    rl.al_preformulario_completado(flujo, t(0), "e1", settings)
    rl.al_lead_calificado(flujo, t(1), "e2", "AUTOMATICA", settings)
    rl.al_lead_calificado(flujo, t(50), "e2b", "AUTOMATICA", settings)

    assert _paso(flujo, rl.CALIFICAR_LEAD).latencia_ms == 1000
