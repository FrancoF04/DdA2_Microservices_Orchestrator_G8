"""Arranque local: `python -m app [--reload] [--port 8080]`.

En Windows, el event loop por defecto de asyncio (ProactorEventLoop) no es
compatible con el driver async de psycopg, así que se fuerza
SelectorEventLoop. En Linux (Docker, Render) se usa el de siempre.
Equivale a: uvicorn app.main:app --loop asyncio:SelectorEventLoop
"""

from __future__ import annotations

import argparse
import os
import sys

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m app", description="Levanta el orquestador.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8080")))
    parser.add_argument("--reload", action="store_true", help="Reinicia al cambiar el código.")
    args = parser.parse_args()

    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        loop="asyncio:SelectorEventLoop" if sys.platform == "win32" else "auto",
    )


if __name__ == "__main__":
    main()
