import uuid

import pytest
from fastapi.testclient import TestClient

from app.middleware.correlation_id import HEADER, resolver_correlation_id


def test_respeta_el_correlation_id_valido(client: TestClient) -> None:
    respuesta = client.get("/health", headers={HEADER: "abc-123.X_y"})
    assert respuesta.headers[HEADER] == "abc-123.X_y"


def test_genera_uno_si_no_viene(client: TestClient) -> None:
    respuesta = client.get("/health")
    uuid.UUID(respuesta.headers[HEADER])  # no lanza: es un UUID


@pytest.mark.parametrize("valor", ["con espacio", "linea\nnueva", "x" * 65, ""])
def test_descarta_valores_inseguros(valor: str) -> None:
    resultado = resolver_correlation_id(valor)
    assert resultado != valor
    uuid.UUID(resultado)
