"""NEXO Worker entrypoint."""
from __future__ import annotations

import os

import uvicorn

from .logging import setup_logging


def main() -> int:
    setup_logging(os.environ.get("NEXO_LOG_LEVEL", "info"))
    uvicorn.run(
        "nexo_worker.main:app",
        host=os.environ.get("NEXO_WORKER_HOST", "0.0.0.0"),
        port=int(os.environ.get("NEXO_WORKER_PORT", "9100")),
        log_level=os.environ.get("NEXO_LOG_LEVEL", "info"),
        workers=1,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
