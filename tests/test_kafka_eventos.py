from app.config import Settings
from app.kafka.config import argumentos_seguridad
from app.kafka.eventos import armar_sobre, extraer_payload


def test_sobre_tiene_los_campos_del_contrato() -> None:
    sobre = armar_sobre("ResumenSolicitado", {"leadId": "L1"}, correlation_id="C1")

    assert sobre["eventType"] == "ResumenSolicitado"
    assert sobre["version"] == 1
    assert sobre["correlationId"] == "C1"
    assert sobre["occurredAt"] == sobre["timestamp"]
    assert sobre["payload"] == {"leadId": "L1"}
    assert sobre["eventId"]


def test_extrae_payload_con_y_sin_sobre() -> None:
    assert extraer_payload({"payload": {"leadId": "L1"}}) == {"leadId": "L1"}
    # lead-calificado de calificacion llega plano (I1)
    assert extraer_payload({"leadId": "L1", "nivel": "POTABLE"})["nivel"] == "POTABLE"


def test_sin_protocolo_no_hay_argumentos_de_seguridad() -> None:
    assert argumentos_seguridad(Settings(_env_file=None)) == {}


def test_sasl_ssl_arma_ssl_context() -> None:
    settings = Settings(
        _env_file=None,
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_mechanism="PLAIN",
        kafka_sasl_username="u",
        kafka_sasl_password="p",
    )
    args = argumentos_seguridad(settings)
    assert args["security_protocol"] == "SASL_SSL"
    assert args["sasl_plain_username"] == "u"
    assert "ssl_context" in args
