from fastapi.testclient import TestClient

from app.middleware.correlation_id import HEADER


def test_ruta_inexistente_devuelve_problem_detail(client: TestClient) -> None:
    respuesta = client.get("/no-existe", headers={HEADER: "cid-404"})

    assert respuesta.status_code == 404
    assert respuesta.headers["content-type"].startswith("application/problem+json")
    cuerpo = respuesta.json()
    assert cuerpo["codigo"] == "RECURSO_NO_ENCONTRADO"
    assert cuerpo["status"] == 404
    assert cuerpo["correlationId"] == "cid-404"
