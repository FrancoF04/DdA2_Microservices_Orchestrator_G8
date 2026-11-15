"""Comando `resumen-solicitado`: arma el payload a partir del detalle del lead.

El contrato es el mismo que publicaba leads (`ResumenSolicitado`), así ia-resumen
no se entera de quién lo publica. Los datos de contacto y del vehículo no viajan:
ia-resumen los consulta a clientes y vehiculos por los ids.
"""

from __future__ import annotations

from typing import Any

# Campos que `GET /leads/{id}` tiene que devolver para poder armar el comando.
_OBLIGATORIOS = ("id", "cliente_id", "vehiculo_id", "anticipo", "solicita_financiacion", "permuta_tiene")


def armar_payload_resumen(lead: dict[str, Any]) -> dict[str, Any]:
    faltan = [campo for campo in _OBLIGATORIOS if lead.get(campo) is None]
    if faltan:
        raise ValueError(f"El detalle del lead no trae {', '.join(faltan)}.")
    return {
        "leadId": str(lead["id"]),
        "clienteId": str(lead["cliente_id"]),
        "vehiculoId": str(lead["vehiculo_id"]),
        "anticipo": lead["anticipo"],
        "solicitaFinanciacion": lead["solicita_financiacion"],
        "patente": lead.get("patente"),
        "permuta": {
            "tienePermuta": lead["permuta_tiene"],
            "descripcion": lead.get("permuta_descripcion"),
            "valorDeclarado": lead.get("permuta_valor_declarado"),
        },
        "ubicacion": {
            "provincia": lead.get("provincia"),
            "localidad": lead.get("localidad"),
        },
    }
