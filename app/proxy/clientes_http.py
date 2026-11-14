"""Un cliente HTTP por servicio destino, con autoreparación.

- **Aislamiento:** cada servicio tiene su propio pool de conexiones. Si uno se
  traba (por ejemplo, un servicio de Render despertando), no consume las
  conexiones de los demás.
- **Autoreparación:** después de un timeout, el cliente de ese servicio se
  reemplaza por uno nuevo. Se observó que, tras un timeout durante el arranque en
  frío de un servicio en Render, el pool quedaba inutilizable (todas las
  requests siguientes vencían) mientras un cliente nuevo respondía en 0,5 s.
- **Conexiones ociosas cortas:** `keepalive_expiry` bajo para no reutilizar
  conexiones que el otro extremo ya cerró.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

KEEPALIVE_EXPIRY_SEGUNDOS = 20.0
ESPERA_CIERRE_CLIENTE_VIEJO_SEGUNDOS = 120.0


class ClientesHttp:
    def __init__(self, transporte: Optional[httpx.AsyncBaseTransport] = None) -> None:
        self._transporte = transporte
        self._clientes: dict[str, httpx.AsyncClient] = {}
        self._viejos: dict[asyncio.Task, httpx.AsyncClient] = {}
        self.reinicios = 0

    def _crear(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=self._transporte,
            follow_redirects=False,
            limits=httpx.Limits(
                max_connections=50,
                max_keepalive_connections=10,
                keepalive_expiry=KEEPALIVE_EXPIRY_SEGUNDOS,
            ),
        )

    def para(self, servicio: str) -> httpx.AsyncClient:
        cliente = self._clientes.get(servicio)
        if cliente is None:
            cliente = self._clientes[servicio] = self._crear()
        return cliente

    def reiniciar(self, servicio: str, motivo: str) -> None:
        """Reemplaza el cliente del servicio; el viejo se cierra cuando terminen sus requests."""
        viejo = self._clientes.pop(servicio, None)
        if viejo is None:
            return
        self.reinicios += 1
        logger.warning("Reiniciando el cliente HTTP de '%s' (%s).", servicio, motivo)
        # Un transporte inyectado (tests) es compartido: no se cierra.
        if self._transporte is None:
            tarea = asyncio.create_task(self._cerrar_luego(viejo))
            self._viejos[tarea] = viejo
            tarea.add_done_callback(lambda t: self._viejos.pop(t, None))

    async def _cerrar_luego(self, cliente: httpx.AsyncClient) -> None:
        await asyncio.sleep(ESPERA_CIERRE_CLIENTE_VIEJO_SEGUNDOS)
        await cliente.aclose()

    async def cerrar(self) -> None:
        pendientes = list(self._viejos.items())
        for tarea, _ in pendientes:
            tarea.cancel()
        for _, cliente in pendientes:
            await cliente.aclose()
        for cliente in self._clientes.values():
            await cliente.aclose()
        self._clientes.clear()
