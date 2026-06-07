"""Base entity for EEBUS integration."""

from __future__ import annotations

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import EebusCoordinator

# Device identity registered in the HA device registry.
# The EEBUS bridge is currently purpose-built for the Bosch Compress 5800i;
# update these if the integration is extended to support other devices.
_DEVICE_MANUFACTURER = "Bosch"
_DEVICE_MODEL = "Compress 5800i"


class EebusEntity(CoordinatorEntity[EebusCoordinator]):
    """Base class for EEBUS entities."""

    _attr_has_entity_name = True

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize the entity."""
        super().__init__(coordinator)
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, coordinator.ski)},
            name=f"{_DEVICE_MANUFACTURER} {_DEVICE_MODEL}",
            manufacturer=_DEVICE_MANUFACTURER,
            model=_DEVICE_MODEL,
        )
