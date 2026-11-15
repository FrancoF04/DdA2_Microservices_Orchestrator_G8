"""Flujo CONSULTA_MULTAS_VEHICULO: las multas de una unidad en stock (docs/02-propuesta-orquestador.md §4.9).

No es parte del flujo del lead. Hay un flujo por vehículo (la clave de negocio es el `vehiculoId`) y se
reabre cada vez que hay que consultar de nuevo:

    ConsultarMultas (a multas) ──► CONSULTANDO_MULTAS
    MultasConsultadas ─► GuardarMultas (PUT a vehiculos) ──► ACTUALIZANDO_VEHICULO
    (GuardarMultas entregado) ──► COMPLETADO

    ConsultaMultasFallida reintentable ─► ConsultarMultas de nuevo, con espera creciente (máx. N)
    Desvíos: FALLIDO (error no reintentable, intentos agotados, DLT, comando no entregado) y EXPIRADO.

Datos personales: el resultado del scraper (actas, importes, lugares) no se guarda acá. Solo viaja
a vehiculos, y mientras el comando está pendiente en el outbox; el flujo conserva estado y tiempos.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from app.config import Settings
from app.db.models import Flujo, FlujoPaso

TIPO = "CONSULTA_MULTAS_VEHICULO"

# Estados
CONSULTANDO_MULTAS = "CONSULTANDO_MULTAS"
ACTUALIZANDO_VEHICULO = "ACTUALIZANDO_VEHICULO"
COMPLETADO = "COMPLETADO"
FALLIDO = "FALLIDO"
EXPIRADO = "EXPIRADO"

# Estados que esperan algo y por lo tanto tienen vencimiento.
ESTADOS_EN_CURSO = (CONSULTANDO_MULTAS, ACTUALIZANDO_VEHICULO)

# Pasos
CONSULTAR_MULTAS = "CONSULTAR_MULTAS"
GUARDAR_MULTAS = "GUARDAR_MULTAS"

# Estado de un paso
PENDIENTE, OK, ERROR, TIMEOUT = "PENDIENTE", "OK", "ERROR", "TIMEOUT"

# Motivos de `ConsultaMultasFallida`
SITIO_NO_DISPONIBLE = "SITIO_NO_DISPONIBLE"
ESTRUCTURA_NO_RECONOCIDA = "ESTRUCTURA_NO_RECONOCIDA"
PATENTE_INVALIDA = "PATENTE_INVALIDA"

_MOTIVO_VENCIMIENTO = {
    CONSULTANDO_MULTAS: "multas no respondió a tiempo (no llegó MultasConsultadas)",
    ACTUALIZANDO_VEHICULO: "no se pudo entregar el resultado a vehiculos a tiempo",
}


class ConsultaEnCurso(Exception):
    """Ya hay una consulta de multas esperando respuesta para esa unidad."""


def _paso(flujo: Flujo, nombre: str) -> Optional[FlujoPaso]:
    for paso in reversed(flujo.pasos):
        if paso.paso == nombre:
            return paso
    return None


def _abrir_paso(
    flujo: Flujo, nombre: str, servicio: str, canal: str, momento: datetime, referencia: str, intento: int = 1
) -> None:
    flujo.pasos.append(
        FlujoPaso(
            paso=nombre,
            servicio=servicio,
            canal=canal,
            estado=PENDIENTE,
            intento=intento,
            iniciado_en=momento,
            referencia=referencia,
        )
    )


def _cerrar_paso(flujo: Flujo, nombre: str, estado: str, momento: datetime, referencia: str, detalle: Optional[str]) -> None:
    paso = _paso(flujo, nombre)
    if paso is None or paso.estado == OK:
        return
    paso.estado = estado
    paso.respondido_en = momento
    paso.referencia = referencia
    paso.detalle_error = detalle[:500] if detalle else None
    paso.latencia_ms = max(0, int((momento - paso.iniciado_en).total_seconds() * 1000))


def _fallar(flujo: Flujo, detalle: str) -> None:
    flujo.estado = FALLIDO
    flujo.vence_en = None
    flujo.ultimo_error = detalle[:500]


def _esperando_respuesta(flujo: Flujo) -> bool:
    """Hay una consulta sin responder: el flujo la espera, o venció y todavía puede llegar la respuesta."""
    paso = _paso(flujo, CONSULTAR_MULTAS)
    return (
        paso is not None
        and paso.estado in (PENDIENTE, TIMEOUT)
        and flujo.estado in (CONSULTANDO_MULTAS, EXPIRADO)
    )


# ---------------------------------------------------------------- inicio

def iniciar(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> None:
    """Abre una consulta nueva (el flujo es nuevo o termina de cerrar la anterior).

    Los pasos anteriores quedan como historial. Si hay una consulta en curso, no se pisa.
    """
    if flujo.estado in ESTADOS_EN_CURSO:
        raise ConsultaEnCurso()
    flujo.estado = CONSULTANDO_MULTAS
    flujo.paso_actual = CONSULTAR_MULTAS
    flujo.vence_en = momento + timedelta(seconds=settings.flujo_timeout_multas_segundos)
    flujo.intentos = 0
    flujo.ultimo_error = None
    flujo.finalizado_en = None
    _abrir_paso(flujo, CONSULTAR_MULTAS, "multas", "KAFKA", momento, referencia)


# ---------------------------------------------------------------- eventos

def al_multas_consultadas(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> bool:
    """Llegó el resultado. Devuelve True si hay que entregárselo a vehiculos (una sola vez por consulta)."""
    if not _esperando_respuesta(flujo):
        return False  # evento repetido o fuera de tiempo: no se vuelve a entregar
    _cerrar_paso(flujo, CONSULTAR_MULTAS, OK, momento, referencia, None)
    _abrir_paso(flujo, GUARDAR_MULTAS, "vehiculos", "REST", momento, referencia)
    flujo.estado = ACTUALIZANDO_VEHICULO
    flujo.paso_actual = GUARDAR_MULTAS
    flujo.vence_en = momento + timedelta(seconds=settings.flujo_timeout_multas_segundos)
    flujo.ultimo_error = None
    return True


def al_consulta_fallida(
    flujo: Flujo, momento: datetime, referencia: str, motivo: str, reintentable: bool, settings: Settings
) -> Optional[float]:
    """El scraper no pudo consultar. Devuelve la espera (segundos) antes de volver a pedirlo, o None si no se reintenta."""
    if not _esperando_respuesta(flujo):
        return None
    _cerrar_paso(flujo, CONSULTAR_MULTAS, ERROR, momento, referencia, f"multas no pudo consultar: {motivo}")
    if not reintentable or flujo.intentos + 1 >= settings.multas_max_intentos:
        if reintentable:
            _fallar(flujo, f"multas no pudo consultar tras {flujo.intentos + 1} intentos: {motivo}")
        else:
            _fallar(flujo, f"multas no pudo consultar: {motivo} (no se reintenta)")
        return None
    flujo.intentos += 1
    espera = settings.multas_espera_base_segundos * (2 ** (flujo.intentos - 1))
    flujo.estado = CONSULTANDO_MULTAS
    flujo.paso_actual = CONSULTAR_MULTAS
    flujo.vence_en = momento + timedelta(seconds=espera + settings.flujo_timeout_multas_segundos)
    flujo.ultimo_error = f"reintento {flujo.intentos}: {motivo}"
    _abrir_paso(flujo, CONSULTAR_MULTAS, "multas", "KAFKA", momento, referencia, intento=flujo.intentos + 1)
    return espera


def al_consulta_en_dlt(flujo: Flujo, momento: datetime, referencia: str) -> None:
    """multas agotó los reintentos con `ConsultarMultas`: el comando fue a `crm.dlt`."""
    if not _esperando_respuesta(flujo):
        return
    detalle = "multas no pudo procesar el comando (mensaje enviado a la DLT)"
    _cerrar_paso(flujo, CONSULTAR_MULTAS, ERROR, momento, referencia, detalle)
    _fallar(flujo, detalle)


def al_consulta_no_publicada(flujo: Flujo, momento: datetime, referencia: str, detalle: str) -> None:
    """El orquestador agotó los intentos de publicar `ConsultarMultas` (o no se puede armar: sin patente)."""
    texto = f"No se pudo pedir la consulta de multas: {detalle}"
    _cerrar_paso(flujo, CONSULTAR_MULTAS, ERROR, momento, referencia, texto)
    if flujo.estado in (CONSULTANDO_MULTAS, EXPIRADO):
        _fallar(flujo, texto)


def al_multas_guardadas(flujo: Flujo, momento: datetime, referencia: str) -> None:
    """vehiculos recibió el resultado."""
    _cerrar_paso(flujo, GUARDAR_MULTAS, OK, momento, referencia, None)
    flujo.estado = COMPLETADO
    flujo.paso_actual = None
    flujo.vence_en = None
    flujo.ultimo_error = None
    flujo.finalizado_en = momento


def al_guardado_fallido(flujo: Flujo, momento: datetime, referencia: str, detalle: str) -> None:
    """El orquestador agotó los intentos de entregarle el resultado a vehiculos."""
    texto = f"No se pudo entregar el resultado a vehiculos: {detalle}"
    _cerrar_paso(flujo, GUARDAR_MULTAS, ERROR, momento, referencia, texto)
    _fallar(flujo, texto)


def vencer(flujo: Flujo, ahora: datetime) -> None:
    """Marca el flujo como EXPIRADO porque su paso actual no se resolvió a tiempo."""
    motivo = _MOTIVO_VENCIMIENTO.get(flujo.estado, "el paso actual no se resolvió a tiempo")
    if flujo.paso_actual:
        paso = _paso(flujo, flujo.paso_actual)
        if paso is not None and paso.estado == PENDIENTE:
            paso.estado = TIMEOUT
            paso.detalle_error = motivo
    flujo.estado = EXPIRADO
    flujo.vence_en = None
    flujo.ultimo_error = motivo
