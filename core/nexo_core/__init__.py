"""NEXO Core — the multi-protocol proxy runtime.

NEXO Core is the server runtime that NEXO instances run. It relays
VLESS / Trojan / Shadowsocks traffic over WebSocket and xHTTP transports,
exposes health/metrics APIs, and is managed remotely by the NEXO Console
through a NEXO Worker.

Derived from the RVG Gateway relay engine; refactored into a clean,
library-style package with explicit state boundaries.
"""
