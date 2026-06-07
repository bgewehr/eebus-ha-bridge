"""Config flow for EEBUS integration."""

from __future__ import annotations

import logging
from typing import Any

import grpc
import grpc.aio
import voluptuous as vol

from homeassistant.config_entries import ConfigFlow, ConfigFlowResult, OptionsFlow
from homeassistant.core import callback

from .const import (
    CONF_DEVICE_SKI,
    CONF_EMSESP_URL,
    CONF_GRPC_HOST,
    CONF_GRPC_PORT,
    DEFAULT_GRPC_PORT,
    DOMAIN,
)

_LOGGER = logging.getLogger(__name__)

CONFIG_RPC_TIMEOUT = 8

STEP_USER_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_GRPC_HOST): str,
        vol.Required(CONF_GRPC_PORT, default=DEFAULT_GRPC_PORT): int,
    }
)

STEP_DEVICE_DATA_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_DEVICE_SKI): str,
    }
)


class EebusConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for EEBUS."""

    VERSION = 1
    DOMAIN = DOMAIN

    def __init__(self) -> None:
        """Initialize."""
        self._host: str = ""
        self._port: int = DEFAULT_GRPC_PORT

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            self._host = user_input[CONF_GRPC_HOST]
            self._port = user_input[CONF_GRPC_PORT]

            channel = grpc.aio.insecure_channel(f"{self._host}:{self._port}")
            try:
                from . import proto_stubs
                stub = proto_stubs.DeviceServiceStub(channel)
                await stub.GetStatus(proto_stubs.Empty(), timeout=CONFIG_RPC_TIMEOUT)
                return await self.async_step_device()
            except Exception:
                _LOGGER.exception(
                    "Failed to connect to EEBUS bridge during config flow at %s:%s",
                    self._host,
                    self._port,
                )
                errors["base"] = "cannot_connect"
            finally:
                await channel.close()

        return self.async_show_form(
            step_id="user",
            data_schema=STEP_USER_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_device(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle device selection step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            # Normalize before validation so the stored SKI is already canonical.
            # SKIs are often displayed with spaces in documentation (e.g. "AB CD EF").
            ski = user_input[CONF_DEVICE_SKI].strip().replace(" ", "").upper()

            if not ski:
                errors["base"] = "invalid_ski_empty"
            elif len(ski) < 10:
                errors["base"] = "invalid_ski_too_short"
            elif len(ski) > 40:
                errors["base"] = "invalid_ski_too_long"
            else:
                await self.async_set_unique_id(ski)
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"EEBUS {ski[:8]}",
                    data={
                        CONF_GRPC_HOST: self._host,
                        CONF_GRPC_PORT: self._port,
                        CONF_DEVICE_SKI: ski,
                    },
                )

        return self.async_show_form(
            step_id="device",
            data_schema=STEP_DEVICE_DATA_SCHEMA,
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reconfiguration."""
        errors: dict[str, str] = {}

        if user_input is not None:
            channel = grpc.aio.insecure_channel(
                f"{user_input[CONF_GRPC_HOST]}:{user_input[CONF_GRPC_PORT]}"
            )
            try:
                from . import proto_stubs
                stub = proto_stubs.DeviceServiceStub(channel)
                await stub.GetStatus(proto_stubs.Empty(), timeout=CONFIG_RPC_TIMEOUT)
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates={
                        CONF_GRPC_HOST: user_input[CONF_GRPC_HOST],
                        CONF_GRPC_PORT: user_input[CONF_GRPC_PORT],
                    },
                )
            except Exception:
                _LOGGER.exception(
                    "Failed to connect to EEBUS bridge during reconfigure at %s:%s",
                    user_input[CONF_GRPC_HOST],
                    user_input[CONF_GRPC_PORT],
                )
                errors["base"] = "cannot_connect"
            finally:
                await channel.close()

        entry = self._get_reconfigure_entry()
        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_GRPC_HOST, default=entry.data.get(CONF_GRPC_HOST, "")): str,
                    vol.Required(CONF_GRPC_PORT, default=entry.data.get(CONF_GRPC_PORT, DEFAULT_GRPC_PORT)): int,
                }
            ),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry):  # type: ignore[override]
        """Return options flow."""
        return EebusOptionsFlow()


class EebusOptionsFlow(OptionsFlow):
    """Options flow for configuring EMS-ESP integration."""

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Manage options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current_url = self.config_entry.options.get(CONF_EMSESP_URL, "")
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(CONF_EMSESP_URL, default=current_url): str,
                }
            ),
            description_placeholders={
                "example": "http://ems-esp",
            },
        )
