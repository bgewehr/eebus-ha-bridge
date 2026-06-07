"""Diagnostics for the EEBUS integration."""

from __future__ import annotations

from typing import Any

from homeassistant.core import HomeAssistant

from . import EebusConfigEntry
from .const import CONF_DEVICE_SKI, CONF_GRPC_HOST, CONF_GRPC_PORT


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant,
    entry: EebusConfigEntry,
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data

    return {
        "config": {
            CONF_GRPC_HOST: entry.data.get(CONF_GRPC_HOST),
            CONF_GRPC_PORT: entry.data.get(CONF_GRPC_PORT),
            CONF_DEVICE_SKI: "**REDACTED**",
        },
        "coordinator_data": dict(coordinator.data) if coordinator.data else None,
    }
