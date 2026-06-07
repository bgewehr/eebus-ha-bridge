"""Number entities for EEBUS integration."""

from __future__ import annotations

from homeassistant.components.number import NumberDeviceClass, NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EntityCategory, UnitOfPower, UnitOfTime
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import EebusCoordinator
from .entity import EebusEntity

PARALLEL_UPDATES = 0  # Coordinator-based, no per-entity polling


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up EEBUS number entities."""
    coordinator: EebusCoordinator = entry.runtime_data
    async_add_entities([
        EebusLPCLimitNumber(coordinator),
        EebusLPCDurationNumber(coordinator),
        EebusFailsafeLimitNumber(coordinator),
        EebusFailsafeDurationNumber(coordinator),
    ])


class EebusLPCLimitNumber(EebusEntity, NumberEntity):
    """Number entity for setting LPC consumption limit.

    Gold: device_class, translation_key, entity_category CONFIG.
    """

    _attr_device_class = NumberDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_mode = NumberMode.BOX
    _attr_native_min_value = 0
    _attr_native_max_value = 32000
    _attr_native_step = 100
    _attr_translation_key = "lpc_limit"
    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.ski}_lpc_limit"

    @property
    def native_value(self) -> float | None:
        """Return current limit value."""
        if self.coordinator.data is None:
            return None
        limit = self.coordinator.data.get("consumption_limit")
        if limit is None:
            return None
        return limit.get("value_watts")

    @property
    def available(self) -> bool:
        """Disable entity when LPC is known to be unsupported."""
        if not super().available:
            return False
        if self.coordinator.data is None:
            return False
        return self.coordinator.data.get("lpc_supported") is not False

    async def async_set_native_value(self, value: float) -> None:
        """Set new LPC limit via gRPC."""
        await self.coordinator.async_write_lpc_limit(value)
        await self.coordinator.async_request_refresh()


class EebusLPCDurationNumber(EebusEntity, NumberEntity):
    """Number entity for setting the LPC limit duration.

    Displayed in minutes; sent to device as seconds.
    Range: 5 – 120 min. Spec imposes no limit; 120 min is a sensible cap.
    """

    _attr_native_unit_of_measurement = UnitOfTime.MINUTES
    _attr_mode = NumberMode.BOX
    _attr_native_min_value = 5
    _attr_native_max_value = 120
    _attr_native_step = 5
    _attr_translation_key = "lpc_duration"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.ski}_lpc_duration"

    @property
    def native_value(self) -> float:
        """Return current configured LPC duration in minutes."""
        return self.coordinator.lpc_duration_seconds / 60

    @property
    def available(self) -> bool:
        """Available when LPC is supported."""
        if not super().available:
            return False
        if self.coordinator.data is None:
            return False
        return self.coordinator.data.get("lpc_supported") is not False

    async def async_set_native_value(self, value: float) -> None:
        """Convert minutes to seconds and store in coordinator."""
        await self.coordinator.async_write_lpc_duration(int(round(value * 60)))


class EebusFailsafeLimitNumber(EebusEntity, NumberEntity):
    """Number entity for setting failsafe limit.

    Gold: entity_category CONFIG, entity_disabled_by_default.
    """

    _attr_device_class = NumberDeviceClass.POWER
    _attr_native_unit_of_measurement = UnitOfPower.WATT
    _attr_mode = NumberMode.BOX
    _attr_native_min_value = 0
    _attr_native_max_value = 32000
    _attr_native_step = 100
    _attr_translation_key = "failsafe_limit"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False  # Gold: less popular entities disabled

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.ski}_failsafe_limit"

    @property
    def native_value(self) -> float | None:
        """Return current failsafe limit."""
        if self.coordinator.data is None:
            return None
        failsafe = self.coordinator.data.get("failsafe_limit")
        if failsafe is None:
            return None
        return failsafe.get("value_watts")

    @property
    def available(self) -> bool:
        """Disable entity when failsafe is known to be unsupported."""
        if not super().available:
            return False
        if self.coordinator.data is None:
            return False
        return self.coordinator.data.get("failsafe_supported") is not False

    async def async_set_native_value(self, value: float) -> None:
        """Set new failsafe limit via gRPC."""
        await self.coordinator.async_write_failsafe_limit(value)
        await self.coordinator.async_request_refresh()


class EebusFailsafeDurationNumber(EebusEntity, NumberEntity):
    """Number entity for setting the failsafe minimum duration.

    Displayed in hours; sent to device as seconds.
    EEBUS spec mandates 2 h – 24 h (eebus-go enforces this).
    Step: 1 h.
    """

    _attr_native_unit_of_measurement = UnitOfTime.HOURS
    _attr_mode = NumberMode.BOX
    _attr_native_min_value = 2
    _attr_native_max_value = 24
    _attr_native_step = 1
    _attr_translation_key = "failsafe_duration"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_entity_registry_enabled_default = False

    def __init__(self, coordinator: EebusCoordinator) -> None:
        """Initialize."""
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.ski}_failsafe_duration"

    @property
    def native_value(self) -> float:
        """Return current configured failsafe minimum duration in hours."""
        # Prefer the value read from the device if available.
        if self.coordinator.data:
            failsafe = self.coordinator.data.get("failsafe_limit")
            if failsafe and failsafe.get("duration_minimum_seconds"):
                return failsafe["duration_minimum_seconds"] / 3600
        return self.coordinator.failsafe_duration_minimum_seconds / 3600

    @property
    def available(self) -> bool:
        """Available when failsafe is supported."""
        if not super().available:
            return False
        if self.coordinator.data is None:
            return False
        return self.coordinator.data.get("failsafe_supported") is not False

    async def async_set_native_value(self, value: float) -> None:
        """Convert hours to seconds and write to device."""
        await self.coordinator.async_write_failsafe_duration(int(round(value * 3600)))
