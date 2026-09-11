"""NEXO Console API version info."""
from __future__ import annotations

import os

APP_NAME = "NEXO Console"


def version() -> str:
    return os.environ.get("NEXO_CONSOLE_VERSION", "1.0.0")


def info() -> dict:
    return {
        "name": APP_NAME,
        "version": version(),
        "build": os.environ.get("NEXO_CONSOLE_BUILD", "dev"),
    }
