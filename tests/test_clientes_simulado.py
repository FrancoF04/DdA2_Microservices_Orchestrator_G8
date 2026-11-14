"""El simulador de clientes tiene que responder lo que esperan leads, calificacion,
ia-resumen y el front (docs/01-relevamiento-ecosistema.md §3.7)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from dev.clientes_simulado import main as simulado

CLIENTE = {
    "nombre": "Martina",
    "apellido": "López",
    "email": "martina@test.com",
    "provincia": "BUENOS_AIRES",
    "cuil": "20123456786",
    "consentimiento": True,
}


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setattr(simulado, "KAFKA_BOOTSTRAP", "")
    monkeypatch.setattr(simulado, "CLAVE_LEADS", "")
    monkeypatch.setattr(simulado, "CLAVE_CALIFICACION", "")
    with TestClient(simulado.app) as c:
        yield c


def test_alta_devuelve_201_con_cliente_id_como_espera_leads(client: TestClient) -> None:
    respuesta = client.post("/clientes", json=CLIENTE)
    assert respuesta.status_code == 201
    assert "clienteId" in respuesta.json()


def test_detalle_enmascara_el_cuil(client: TestClient) -> None:
    cliente_id = client.post("/clientes", json=CLIENTE).json()["clienteId"]

    datos = client.get(f"/clientes/{cliente_id}").json()

    assert datos["id"] == cliente_id
    assert datos["nombre"] == "Martina"
    assert "20123456786" not in datos["cuil"]


def test_situacion_crediticia_es_deterministica(client: TestClient) -> None:
    con_cuil = client.post("/clientes", json=CLIENTE).json()["clienteId"]
    sin_cuil = client.post(
        "/clientes", json={k: v for k, v in CLIENTE.items() if k not in ("cuil", "consentimiento")}
    ).json()["clienteId"]

    # Penúltimo dígito de 20123456786 es 8 → 8 % 6 + 1 = 3
    assert client.get(f"/clientes/{con_cuil}/situacion-crediticia").json()["situacion"] == 3
    assert client.get(f"/clientes/{sin_cuil}/situacion-crediticia").json()["situacion"] is None


def test_valida_campos_obligatorios_como_problem_detail(client: TestClient) -> None:
    respuesta = client.post("/clientes", json={"nombre": "x"})
    assert respuesta.status_code == 422
    campos = {e["campo"] for e in respuesta.json()["errores"]}
    assert {"apellido", "email", "provincia"} <= campos


def test_exige_la_clave_si_esta_configurada(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(simulado, "CLAVE_LEADS", "secreta")
    assert client.post("/clientes", json=CLIENTE).status_code == 401
    assert client.post("/clientes", json=CLIENTE, headers={"X-Api-Key": "secreta"}).status_code == 201


def test_cliente_inexistente_da_404(client: TestClient) -> None:
    assert client.get("/clientes/no-existe").status_code == 404
