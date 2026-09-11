"""NEXO Console API entrypoint."""
from __future__ import annotations

import os

import uvicorn

from .logging import setup_logging


def main() -> int:
    setup_logging()
    uvicorn.run(
        "nexo_console.main:app",
        host=os.environ.get("NEXO_CONSOLE_HOST", "0.0.0.0"),
        port=int(os.environ.get("NEXO_CONSOLE_PORT", os.environ.get("PORT", "8080"))),
        log_level=os.environ.get("NEXO_LOG_LEVEL", "info"),
        ws="auto",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
