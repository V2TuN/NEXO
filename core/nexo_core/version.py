"""Runtime information for NEXO Core.

Exposes version / build / commit so the Console can detect available
versions and support update / rollback flows.
"""
from __future__ import annotations

import os

APP_NAME = "NEXO Core"


def version() -> str:
    return os.environ.get("NEXO_VERSION", "1.0.0")


def build() -> str:
    return os.environ.get("NEXO_BUILD", "dev")


def commit() -> str:
    return os.environ.get("NEXO_COMMIT", "unknown")


def info() -> dict:
    return {
        "name": APP_NAME,
        "version": version(),
        "build": build(),
        "commit": commit(),
    }
