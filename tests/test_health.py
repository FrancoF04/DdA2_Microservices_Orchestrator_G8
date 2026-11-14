from fastapi.testclient import TestClient


def test_health_sin_dependencias_esta_up(client: TestClient) -> None:
    respuesta = client.get("/health")

    assert respuesta.status_code == 200
    cuerpo = respuesta.json()
    assert cuerpo["status"] == "UP"
    assert cuerpo["componentes"]["db"]["status"] == "DESHABILITADO"
    assert cuerpo["componentes"]["kafka"]["status"] == "DESHABILITADO"
