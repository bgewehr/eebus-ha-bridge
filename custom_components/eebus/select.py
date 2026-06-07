"""Select entities for EEBUS integration."""

from __future__ import annotations

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import EebusCoordinator
from .entity import EebusEntity

PARALLEL_UPDATES = 0  # Coordinator-based, no per-entity polling

# SG-Ready option keys — must match translation state keys in strings.json / en.json.
SG_READY_OPTIONS: list[str] = ["normal", "encourage", "force"]


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up EEBUS select entities."""
    coordinator: EebusCoordinator = entry.runtime_data
    async_add_entities([EebusSGReadySelect(coordinator)])


class EebusSGReadySelect(EebusEntity, SelectEntity):
    """Select entity for SG-Ready mode control via EMS-ESP.

    Controls the Bosch Compress 5800i DHW via EMS-ESP REST API:
      normal   (Mode 2) → restore boiler/dhw/seltemp to base value
      encourage (Mode 3) → raise boiler/dhw/seltemp by 5 K
      force    (Mode 4) → boiler/dhw/onetime = 1 (one-time DHW charge)

    SG-Ready Mode 1 (block/limit) is handled by EEBUS LPC separately.
    Requires EMS-ESP URL configured in integration options.
    Entity is disabled by default — enable it after configuring the EMS-ESP URL.
    """

    _attr_translation_key = "sg_ready_mode"
    _attr_options = SG_READY_OPTIONS
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False  # needs EMS-ESP URL configured

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.ski}_sg_ready_mode"

    @property
    def available(self) -> bool:
        """Available only when EMS-ESP URL is configured."""
        if not super().available:
            return False
        return bool(self.coordinator.emsesp_url)

    @property
    def current_option(self) -> str | None:
        """Return current SG-Ready mode (derived from boiler DHW state)."""
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.get("sg_ready_mode")

    async def async_select_option(self, option: str) -> None:
        """Set SG-Ready mode via EMS-ESP."""
        try:
            await self.coordinator.async_set_sg_ready_mode(option)
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("SG-Ready: failed to set mode %r: %s", option, err)
            return
        # Coordinator.data was updated optimistically; notify HA immediately
        # so the entity does not show a stale value until the next 30 s poll.
        self.async_write_ha_state()
