"""Configuración del orquestador, leída de variables de entorno (y de `.env` en local)."""

from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # --- app ---
    app_nombre: str = "orquestador"
    port: int = 8080
    log_level: str = "INFO"
    cors_allowed_origins: str = "http://localhost:5173,http://localhost:3000"

    # --- servicios destino (URL base, sin el recurso) ---
    leads_url: str = "http://localhost:8000"
    clientes_url: str = "http://localhost:8082"
    vehiculos_url: str = "http://localhost:8083"
    calificacion_url: str = "http://localhost:8084"
    monitor_url: str = "http://localhost:8085"
    ia_resumen_url: str = "http://localhost:8001"
    auth_url: Optional[str] = None
    multas_url: Optional[str] = None
    # Tiempo máximo esperando la respuesta del servicio. En Render (plan free)
    # un servicio dormido tarda hasta ~1 min en despertar: usar 60 allá.
    proxy_timeout_segundos: float = 30.0
    proxy_timeout_conexion_segundos: float = 10.0
    # Reintentos ante error de conexión, solo en GET/HEAD/OPTIONS (sin body).
    proxy_reintentos_get: int = 1

    # --- base de datos (Supabase, session pooler 5432) ---
    database_url: Optional[str] = None
    db_schema: str = "orquestador"
    db_pool_size: int = 3

    # --- kafka ---
    kafka_enabled: bool = True
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_consumer_group: str = "orquestador"
    kafka_client_id: str = "orquestador"
    kafka_security_protocol: Optional[str] = None
    kafka_sasl_mechanism: Optional[str] = None
    kafka_sasl_username: Optional[str] = None
    kafka_sasl_password: Optional[str] = None
    kafka_ssl_cafile: Optional[str] = None
    kafka_reintento_conexion_segundos: float = 15.0
    # Tópicos que escucha el orquestador para seguir el flujo del lead (los publican otros servicios).
    kafka_topico_preformulario_completado: str = "preformulario-completado"
    kafka_topico_lead_calificado: str = "lead-calificado"
    kafka_topico_resumen_solicitado: str = "resumen-solicitado"
    kafka_topico_resumen_generado: str = "resumen-generado"
    kafka_topico_preformulario_dlt: str = "preformulario-completado-dlt"

    # --- flujos ---
    # Tiempo máximo de cada paso antes de marcar el flujo como EXPIRADO.
    flujo_timeout_calificacion_segundos: int = 120
    flujo_timeout_resumen_segundos: int = 300
    # Cada cuánto se buscan flujos vencidos.
    flujo_intervalo_vencimiento_segundos: float = 15.0
    auditoria_retencion_dias: int = 7

    # --- autenticación (ver docs/06-autenticacion.md) ---
    # Si está habilitada, las rutas protegidas exigen el JWT que emite el servicio auth
    # (validado acá, localmente) o la credencial de un servicio. Con auth_habilitada=true el
    # orquestador NO arranca sin JWT_SECRET. Solo se apaga (false) en desarrollo y en tests.
    auth_habilitada: bool = True
    # Mismo secreto que JWT_SECRET de AuthMicroservice (HS256). Mínimo 32 caracteres.
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    # Credenciales de los servicios que llaman a otros a través del orquestador, como
    # "leads=<clave>,calificacion=<clave>,ia-resumen=<clave>". Cada clave: 32+ caracteres y distinta.
    claves_servicio: Optional[str] = None

    @field_validator(
        "auth_url",
        "multas_url",
        "jwt_secret",
        "claves_servicio",
        "database_url",
        "kafka_security_protocol",
        "kafka_sasl_mechanism",
        "kafka_sasl_username",
        "kafka_sasl_password",
        "kafka_ssl_cafile",
        mode="before",
    )
    @classmethod
    def _vacio_a_none(cls, valor: object) -> object:
        """En un `.env`, `VAR=` llega como string vacío: se trata como no configurada."""
        if isinstance(valor, str) and not valor.strip():
            return None
        return valor

    @property
    def cors_origins(self) -> list[str]:
        return [o.strip() for o in self.cors_allowed_origins.split(",") if o.strip()]

    @property
    def claves_servicio_por_nombre(self) -> dict[str, str]:
        """`CLAVES_SERVICIO` ("leads=abc,calificacion=def") como {nombre: clave}."""
        resultado: dict[str, str] = {}
        for par in (self.claves_servicio or "").split(","):
            if not par.strip():
                continue
            nombre, separador, clave = par.partition("=")
            if not separador or not nombre.strip() or not clave.strip():
                raise ValueError("CLAVES_SERVICIO tiene que ser 'servicio=clave,servicio=clave'.")
            resultado[nombre.strip().lower()] = clave.strip()
        return resultado


@lru_cache
def get_settings() -> Settings:
    return Settings()
