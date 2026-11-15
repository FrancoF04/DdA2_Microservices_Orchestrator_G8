from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import crear_app


@pytest.fixture
def settings() -> Settings:
    # _env_file=None: los tests no leen el .env del desarrollador.
    # auth_habilitada=False: estos tests no son de autenticación (esos están en test_auth_*.py).
    return Settings(_env_file=None, kafka_enabled=False, database_url=None, auth_habilitada=False)


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    with TestClient(crear_app(settings)) as c:
        yield c
