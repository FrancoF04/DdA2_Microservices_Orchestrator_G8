"""Flujo REGISTRO_LEAD: seguimiento del lead desde el preformulario hasta el resumen.

Fase 1b: el orquestador **observa** (no da órdenes). Escucha los eventos que
publican leads, calificacion e ia-resumen y avanza la máquina de estados:

    CALIFICACION_PENDIENTE ──lead-calificado──► CALIFICADO ──resumen-solicitado──►
    RESUMEN_PENDIENTE ──resumen-generado──► COMPLETADO

    Desvíos: FALLIDO (DLT de calificacion) y EXPIRADO (vence el tiempo del paso).

Reglas:
- El flujo nace con `preformulario-completado`, o con el primer evento que
  llegue de ese lead si llegan fuera de orden.
- Nunca retrocede: un evento viejo que llega tarde registra su dato pero no
  cambia el estado (ver `progreso`).
- Un flujo EXPIRADO o FALLIDO se recupera solo si después llega el evento que
  faltaba.
- Las correcciones manuales (`lead-calificado` con origen MANUAL) se registran
  como paso pero no cambian el estado.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import Settings
from app.db.models import Flujo, FlujoPaso

TIPO = "REGISTRO_LEAD"

# Estados
CALIFICACION_PENDIENTE = "CALIFICACION_PENDIENTE"
CALIFICADO = "CALIFICADO"
RESUMEN_PENDIENTE = "RESUMEN_PENDIENTE"
COMPLETADO = "COMPLETADO"
FALLIDO = "FALLIDO"
EXPIRADO = "EXPIRADO"

# Estados que esperan algo y por lo tanto tienen vencimiento.
ESTADOS_EN_CURSO = (CALIFICACION_PENDIENTE, CALIFICADO, RESUMEN_PENDIENTE)

# Pasos
CALIFICAR_LEAD = "CALIFICAR_LEAD"
GENERAR_RESUMEN = "GENERAR_RESUMEN"
CORRECCION_CALIFICACION = "CORRECCION_CALIFICACION"

# Estado de un paso
PENDIENTE, OK, ERROR, TIMEOUT = "PENDIENTE", "OK", "ERROR", "TIMEOUT"

_RANGO_ESTADO = {CALIFICACION_PENDIENTE: 1, CALIFICADO: 2, RESUMEN_PENDIENTE: 3, COMPLETADO: 4}
# Cuánto avanzó el flujo según sus pasos: (paso, terminó OK) -> rango.
_RANGO_PASO = {
    (CALIFICAR_LEAD, False): 1,
    (CALIFICAR_LEAD, True): 2,
    (GENERAR_RESUMEN, False): 3,
    (GENERAR_RESUMEN, True): 4,
}

_MOTIVO_VENCIMIENTO = {
    CALIFICACION_PENDIENTE: "calificacion no respondió a tiempo (no llegó lead-calificado)",
    CALIFICADO: "no se pidió el resumen a tiempo (no se publicó resumen-solicitado)",
    RESUMEN_PENDIENTE: "ia-resumen no respondió a tiempo (no llegó resumen-generado)",
}


def progreso(flujo: Flujo) -> int:
    """Hasta dónde llegó el flujo, mirando el estado y los pasos registrados."""
    rango = _RANGO_ESTADO.get(flujo.estado, 0)
    for paso in flujo.pasos:
        rango = max(rango, _RANGO_PASO.get((paso.paso, paso.estado == OK), 0))
    return rango


def _paso(flujo: Flujo, nombre: str) -> Optional[FlujoPaso]:
    for paso in reversed(flujo.pasos):
        if paso.paso == nombre:
            return paso
    return None


def _abrir_paso(flujo: Flujo, nombre: str, servicio: str, momento: datetime, referencia: str) -> None:
    if _paso(flujo, nombre) is None:
        flujo.pasos.append(
            FlujoPaso(
                paso=nombre,
                servicio=servicio,
                canal="KAFKA",
                estado=PENDIENTE,
                intento=1,
                iniciado_en=momento,
                referencia=referencia,
            )
        )


def _cerrar_paso(
    flujo: Flujo,
    nombre: str,
    servicio: str,
    estado: str,
    momento: datetime,
    referencia: str,
    detalle: Optional[str] = None,
) -> None:
    paso = _paso(flujo, nombre)
    if paso is None:
        # Llegó la respuesta sin haber visto el pedido (fuera de orden): se registra igual.
        paso = FlujoPaso(paso=nombre, servicio=servicio, canal="KAFKA", estado=estado, iniciado_en=momento)
        flujo.pasos.append(paso)
    elif paso.estado == OK:
        return  # ya estaba cerrado; un evento repetido no lo pisa
    paso.estado = estado
    paso.respondido_en = momento
    paso.referencia = referencia
    paso.detalle_error = detalle
    paso.latencia_ms = max(0, int((momento - paso.iniciado_en).total_seconds() * 1000))


def _avanzar(flujo: Flujo, destino: str, vence_en: Optional[datetime], paso_actual: Optional[str]) -> None:
    """Pasa al estado destino solo si no es un retroceso."""
    if _RANGO_ESTADO[destino] < progreso(flujo):
        return
    flujo.estado = destino
    flujo.vence_en = vence_en
    flujo.paso_actual = paso_actual
    flujo.ultimo_error = None
    if destino == COMPLETADO:
        flujo.finalizado_en = datetime.now(timezone.utc)


# ---------------------------------------------------------------- eventos


def al_preformulario_completado(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> None:
    _abrir_paso(flujo, CALIFICAR_LEAD, "calificacion", momento, referencia)
    vence = momento + timedelta(seconds=settings.flujo_timeout_calificacion_segundos)
    _avanzar(flujo, CALIFICACION_PENDIENTE, vence, CALIFICAR_LEAD)


def al_lead_calificado(
    flujo: Flujo, momento: datetime, referencia: str, origen: Optional[str], settings: Settings
) -> bool:
    """Aplica `lead-calificado`. Devuelve True si hay que pedir el resumen a ia-resumen.

    Se pide una sola vez por flujo: ni una corrección manual ni una calificación
    repetida, con el resumen ya pedido, lo vuelven a pedir.
    """
    ya_pedido = progreso(flujo) >= _RANGO_ESTADO[RESUMEN_PENDIENTE]
    if (origen or "").upper() == "MANUAL" and progreso(flujo) >= _RANGO_ESTADO[CALIFICADO]:
        flujo.pasos.append(
            FlujoPaso(
                paso=CORRECCION_CALIFICACION,
                servicio="calificacion",
                canal="KAFKA",
                estado=OK,
                iniciado_en=momento,
                respondido_en=momento,
                latencia_ms=0,
                referencia=referencia,
            )
        )
        return False
    _cerrar_paso(flujo, CALIFICAR_LEAD, "calificacion", OK, momento, referencia)
    # Después de calificar hay que pedir el resumen (lo publica el orquestador o, con el
    # despachador apagado, leads): se espera con el mismo plazo que el resumen.
    vence = momento + timedelta(seconds=settings.flujo_timeout_resumen_segundos)
    _avanzar(flujo, CALIFICADO, vence, None)
    return not ya_pedido


def al_resumen_solicitado(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> None:
    previo = _paso(flujo, GENERAR_RESUMEN)
    if previo is not None and previo.estado in (TIMEOUT, ERROR):
        # Un pedido nuevo después de un fallo (reintento): intento aparte, con su propia latencia.
        flujo.pasos.append(
            FlujoPaso(
                paso=GENERAR_RESUMEN,
                servicio="ia-resumen",
                canal="KAFKA",
                estado=PENDIENTE,
                intento=(previo.intento or 1) + 1,
                iniciado_en=momento,
                referencia=referencia,
            )
        )
    else:
        _abrir_paso(flujo, GENERAR_RESUMEN, "ia-resumen", momento, referencia)
    vence = momento + timedelta(seconds=settings.flujo_timeout_resumen_segundos)
    _avanzar(flujo, RESUMEN_PENDIENTE, vence, GENERAR_RESUMEN)


def al_resumen_generado(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> None:
    _cerrar_paso(flujo, GENERAR_RESUMEN, "ia-resumen", OK, momento, referencia)
    _avanzar(flujo, COMPLETADO, None, None)


def al_preformulario_en_dlt(flujo: Flujo, momento: datetime, referencia: str, settings: Settings) -> None:
    """calificacion agotó sus reintentos con este preformulario."""
    detalle = "calificacion no pudo procesar el preformulario (mensaje enviado a la DLT)"
    _cerrar_paso(flujo, CALIFICAR_LEAD, "calificacion", ERROR, momento, referencia, detalle)
    if progreso(flujo) < _RANGO_ESTADO[CALIFICADO]:
        flujo.estado = FALLIDO
        flujo.vence_en = None
        flujo.ultimo_error = detalle


class ReintentoNoPermitido(Exception):
    """El flujo no está en un estado desde el que se pueda volver a pedir el resumen."""

    def __init__(self, codigo: str, mensaje: str) -> None:
        super().__init__(mensaje)
        self.codigo = codigo
        self.mensaje = mensaje


def preparar_reintento(flujo: Flujo, ahora: datetime, settings: Settings) -> None:
    """Deja el flujo esperando que se vuelva a publicar `resumen-solicitado`."""
    if flujo.estado == COMPLETADO:
        raise ReintentoNoPermitido("FLUJO_COMPLETADO", "El flujo ya terminó: el resumen ya fue generado.")
    if progreso(flujo) < _RANGO_ESTADO[CALIFICADO]:
        raise ReintentoNoPermitido(
            "FLUJO_SIN_CALIFICAR", "El lead todavía no fue calificado: no hay un resumen que pedir."
        )
    flujo.estado = CALIFICADO
    flujo.paso_actual = None
    flujo.vence_en = ahora + timedelta(seconds=settings.flujo_timeout_resumen_segundos)
    flujo.ultimo_error = None
    flujo.finalizado_en = None


def al_resumen_no_publicado(flujo: Flujo, detalle: str) -> None:
    """El orquestador agotó los intentos de publicar `resumen-solicitado`."""
    if progreso(flujo) >= _RANGO_ESTADO[RESUMEN_PENDIENTE]:
        return  # el pedido llegó igual por otro lado: el flujo ya sigue su curso
    flujo.estado = FALLIDO
    flujo.vence_en = None
    flujo.ultimo_error = f"No se pudo publicar resumen-solicitado: {detalle}"[:500]


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
    flujo.intentos += 1
