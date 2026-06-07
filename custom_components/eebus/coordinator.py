"""DataUpdateCoordinator for EEBUS integration."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta
from typing import Any

import aiohttp
import grpc
import grpc.aio

from homeassistant.core import HomeAssistant
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

_LOGGER = logging.getLogger(__name__)

POLL_INTERVAL = timedelta(seconds=30)
RPC_TIMEOUT = 10
RE_REGISTER_NOT_FOUND_STREAK = 4
# After a LPC write, protect the optimistic is_active state for this many seconds
# to prevent the next coordinator poll from reverting it before the device confirms.
LPC_WRITE_PROTECTION_SECS = 8.0
# After a SG-Ready mode write, protect the optimistic sg_ready_mode for this many
# seconds.  The thermostat may take several seconds to confirm the new value.
SG_READY_WRITE_PROTECTION_SECS = 15.0

# Degrees K by which hc1/seltemp is raised for SG-Ready Mode 3 (encourage).
# This mirrors the thermostat's own pvraiseheat parameter (typically 3 K).
SG_READY_SELTEMP_OFFSET_K: float = 3.0

_SG_READY_MODES = frozenset({"normal", "encourage", "force"})


def _is_unimplemented(err: grpc.aio.AioRpcError) -> bool:
    """Return True when gRPC reports method/use case is not implemented."""
    return err.code() == grpc.StatusCode.UNIMPLEMENTED


def _rpc_error_text(err: grpc.aio.AioRpcError) -> str:
    """Build compact debug output for gRPC errors."""
    return f"code={err.code().name} details={err.details()}"



class EebusCoordinator(DataUpdateCoordinator[dict[str, Any]]):
    """Coordinator that manages gRPC connection and data updates."""

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        port: int,
        ski: str,
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name="EEBUS",
            update_interval=POLL_INTERVAL,
        )
        self.host = host
        self.port = port
        # SKI is normalized (strip + no spaces + uppercase) by the config flow
        # before it is persisted in entry.data — no further processing needed here.
        self.ski = ski
        if not self.ski:
            _LOGGER.warning(
                "EEBUS coordinator initialized with empty SKI; data updates will fail",
            )
        self._channel: grpc.aio.Channel | None = None
        self._stream_tasks: list[asyncio.Task] = []  # reserved for future streaming use
        self._was_unavailable: bool = False
        self._heartbeat_supported: bool | None = None
        self._lpc_supported: bool | None = None
        self._failsafe_supported: bool | None = None
        self._ski_registered: bool = False
        self._not_found_streak: int = 0
        self._last_lpc_write: float = 0.0
        self._last_sg_ready_write: float = 0.0
        # seltemp saved before an encourage/force transition so we can restore it.
        self._sg_ready_base_seltemp: float | None = None
        self._emsesp_url: str = ""
        # User-configured durations (in seconds). Defaults match previous hard-coded values.
        self.lpc_duration_seconds: int = 3600
        self.failsafe_duration_minimum_seconds: int = 7200

    def _ensure_channel(self) -> grpc.aio.Channel:
        """Return the shared gRPC channel, creating it on first call."""
        if self._channel is None:
            self._channel = grpc.aio.insecure_channel(f"{self.host}:{self.port}")
        return self._channel

    async def _async_update_data(self) -> dict[str, Any]:
        """Fetch data via gRPC polling."""
        if not self.ski:
            raise UpdateFailed("Device SKI is empty or invalid; check EEBUS integration configuration")

        try:
            channel = self._ensure_channel()
            from . import proto_stubs

            device_stub = proto_stubs.DeviceServiceStub(channel)
            status = await device_stub.GetStatus(proto_stubs.Empty())

            if not self._ski_registered:
                await self._async_register_remote_ski(device_stub, force=False)

            data: dict[str, Any] = {
                "connected": status.running,
                "local_ski": status.local_ski,
                "ski_registered": self._ski_registered,
                "power_watts": None,
                "energy_consumed_kwh": None,
                "energy_produced_kwh": None,
                "energy_consumed_heating_kwh": None,
                "energy_consumed_dhw_kwh": None,
                "grid_frequency_hz": None,
                "power_l1_watts": None,
                "power_l2_watts": None,
                "power_l3_watts": None,
                "current_l1_ampere": None,
                "current_l2_ampere": None,
                "current_l3_ampere": None,
                "voltage_l1_volt": None,
                "voltage_l2_volt": None,
                "voltage_l3_volt": None,
                "consumption_nominal_max_watts": None,
            }
            if self.ski == status.local_ski:
                _LOGGER.warning(
                    "Configured remote SKI %s matches bridge local SKI; monitoring reads will stay empty",
                    self.ski,
                )

            monitoring_stub = proto_stubs.MonitoringServiceStub(channel)
            lpc_stub = proto_stubs.LPCServiceStub(channel)
            request = proto_stubs.DeviceRequest(ski=self.ski)
            # True as soon as any SKI-specific gRPC call returns actual data.
            # Any successful response proves the device is reachable.
            any_call_succeeded = False

            try:
                power = await monitoring_stub.GetPowerConsumption(
                    request, timeout=RPC_TIMEOUT
                )
                data["power_watts"] = power.watts
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS power read for SKI %s succeeded: watts=%s",
                    self.ski,
                    power.watts,
                )
            except grpc.aio.AioRpcError as err:
                data["power_watts"] = None
                _LOGGER.debug(
                    "EEBUS power read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Failed to read power consumption")
                data["power_watts"] = None

            try:
                measurements = await monitoring_stub.GetMeasurements(
                    request, timeout=RPC_TIMEOUT
                )
                data.update(self._extract_standard_measurements(measurements.measurements))
                scoped_energy = self._extract_scoped_energy_kwh(measurements.measurements)
                data["energy_consumed_heating_kwh"] = scoped_energy["heating"]
                data["energy_consumed_dhw_kwh"] = scoped_energy["dhw"]
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS measurement read for SKI %s: power=%s energy_total=%s energy_produced=%s freq=%s heating=%s dhw=%s entries=%s",
                    self.ski,
                    data["power_watts"],
                    data["energy_consumed_kwh"],
                    data["energy_produced_kwh"],
                    data["grid_frequency_hz"],
                    data["energy_consumed_heating_kwh"],
                    data["energy_consumed_dhw_kwh"],
                    len(measurements.measurements),
                )
            except grpc.aio.AioRpcError as err:
                data["energy_consumed_heating_kwh"] = None
                data["energy_consumed_dhw_kwh"] = None
                _LOGGER.debug(
                    "EEBUS scoped energy read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Failed to read scoped energy measurements")
                data["energy_consumed_heating_kwh"] = None
                data["energy_consumed_dhw_kwh"] = None

            try:
                if data["energy_consumed_kwh"] is None:
                    energy = await monitoring_stub.GetEnergyConsumed(
                        request, timeout=RPC_TIMEOUT
                    )
                    data["energy_consumed_kwh"] = energy.kilowatt_hours
                    any_call_succeeded = True
                    _LOGGER.debug(
                        "EEBUS total energy read for SKI %s succeeded: kWh=%s",
                        self.ski,
                        energy.kilowatt_hours,
                    )
            except grpc.aio.AioRpcError as err:
                data["energy_consumed_kwh"] = None
                _LOGGER.debug(
                    "EEBUS total energy read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Failed to read total consumed energy")
                data["energy_consumed_kwh"] = None

            try:
                limit = await lpc_stub.GetConsumptionLimit(
                    request, timeout=RPC_TIMEOUT
                )
                polled_limit = {
                    "value_watts": limit.value_watts,
                    "is_active": limit.is_active,
                    "is_changeable": limit.is_changeable,
                    "duration_seconds": limit.duration_seconds,
                }
                # If we just sent a write, protect the optimistic is_active for a few
                # seconds — the device may not have confirmed the change yet.
                if (
                    time.monotonic() - self._last_lpc_write < LPC_WRITE_PROTECTION_SECS
                    and self.data
                    and self.data.get("consumption_limit") is not None
                ):
                    polled_limit["is_active"] = self.data["consumption_limit"].get(
                        "is_active", polled_limit["is_active"]
                    )
                data["consumption_limit"] = polled_limit
                self._lpc_supported = True
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS consumption limit read for SKI %s: value=%s active=%s changeable=%s",
                    self.ski,
                    limit.value_watts,
                    limit.is_active,
                    limit.is_changeable,
                )
            except grpc.aio.AioRpcError as err:
                data["consumption_limit"] = None
                _LOGGER.debug(
                    "EEBUS consumption limit read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
                if _is_unimplemented(err):
                    self._lpc_supported = False

            try:
                failsafe = await lpc_stub.GetFailsafeLimit(
                    request, timeout=RPC_TIMEOUT
                )
                data["failsafe_limit"] = {
                    "value_watts": failsafe.value_watts,
                    "duration_minimum_seconds": failsafe.duration_minimum_seconds,
                }
                self._failsafe_supported = True
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS failsafe read for SKI %s: value=%s min_duration_s=%s",
                    self.ski,
                    failsafe.value_watts,
                    failsafe.duration_minimum_seconds,
                )
            except grpc.aio.AioRpcError as err:
                data["failsafe_limit"] = None
                _LOGGER.debug(
                    "EEBUS failsafe read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
                if _is_unimplemented(err):
                    self._failsafe_supported = False

            try:
                hb = await lpc_stub.GetHeartbeatStatus(
                    request, timeout=RPC_TIMEOUT
                )
                data["heartbeat_status"] = {
                    "running": hb.running,
                    "within_duration": hb.within_duration,
                }
                data["heartbeat_supported"] = True
                self._heartbeat_supported = True
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS heartbeat status for SKI %s: running=%s within_duration=%s",
                    self.ski,
                    hb.running,
                    hb.within_duration,
                )
            except grpc.aio.AioRpcError as err:
                data["heartbeat_status"] = None
                data["heartbeat_supported"] = self._heartbeat_supported
                _LOGGER.debug(
                    "EEBUS heartbeat read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
                if _is_unimplemented(err):
                    data["heartbeat_supported"] = False
                    self._heartbeat_supported = False
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Failed to read heartbeat status")
                data["heartbeat_status"] = None
                data["heartbeat_supported"] = self._heartbeat_supported

            try:
                nominal_max = await lpc_stub.GetConsumptionNominalMax(
                    request, timeout=RPC_TIMEOUT
                )
                data["consumption_nominal_max_watts"] = nominal_max.watts
                any_call_succeeded = True
                _LOGGER.debug(
                    "EEBUS nominal max consumption for SKI %s: watts=%s",
                    self.ski,
                    nominal_max.watts,
                )
            except grpc.aio.AioRpcError as err:
                data["consumption_nominal_max_watts"] = None
                _LOGGER.debug(
                    "EEBUS nominal max read failed for SKI %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Failed to read nominal max consumption")
                data["consumption_nominal_max_watts"] = None

            data["lpc_supported"] = self._lpc_supported
            data["failsafe_supported"] = self._failsafe_supported

            # Read thermostat hc1 data to derive current SG-Ready mode.
            # Don't let EMS-ESP errors fail the whole EEBUS poll.
            # SG-Ready is controlled via thermostat boost (Mode 4/force) and
            # hc1/seltemp offset (Mode 3/encourage) — NOT via pvmaxcomp.
            hc1 = await self._async_read_emsesp_hc1()
            if hc1 is not None:
                data["sg_ready_hc1_boost"] = hc1.get("boost")
                data["sg_ready_hc1_seltemp"] = hc1.get("seltemp")
                # Outside the write-protection window, re-derive mode from
                # thermostat state.  During the window, keep the optimistic value.
                if time.monotonic() - self._last_sg_ready_write >= SG_READY_WRITE_PROTECTION_SECS:
                    if hc1.get("boost"):
                        data["sg_ready_mode"] = "force"
                    elif (
                        self._sg_ready_base_seltemp is not None
                        and hc1.get("seltemp") is not None
                        and hc1["seltemp"] >= self._sg_ready_base_seltemp + SG_READY_SELTEMP_OFFSET_K - 0.5
                    ):
                        data["sg_ready_mode"] = "encourage"
                    else:
                        data["sg_ready_mode"] = "normal"
                        # Record base seltemp while in normal mode so we can
                        # raise it accurately when switching to encourage.
                        if hc1.get("seltemp") is not None:
                            self._sg_ready_base_seltemp = float(hc1["seltemp"])
                else:
                    # Preserve the optimistic mode written by async_set_sg_ready_mode.
                    data["sg_ready_mode"] = self.data.get("sg_ready_mode") if self.data else None
            else:
                data["sg_ready_hc1_boost"] = self.data.get("sg_ready_hc1_boost") if self.data else None
                data["sg_ready_hc1_seltemp"] = self.data.get("sg_ready_hc1_seltemp") if self.data else None
                data["sg_ready_mode"] = self.data.get("sg_ready_mode") if self.data else None

            # Re-registration streak: any successful gRPC response proves the device
            # is reachable — reset the streak.  Only increment when no call returned
            # data at all, regardless of which specific measurements are available.
            if any_call_succeeded:
                self._not_found_streak = 0
            else:
                self._not_found_streak += 1

            if self._not_found_streak >= RE_REGISTER_NOT_FOUND_STREAK:
                _LOGGER.warning(
                    "EEBUS reads returned NOT_FOUND for %s consecutive polls; forcing remote SKI re-registration for %s",
                    self._not_found_streak,
                    self.ski,
                )
                await self._async_register_remote_ski(device_stub, force=True)
                self._not_found_streak = 0

            _LOGGER.debug(
                "EEBUS poll summary for SKI %s: power=%s energy_total=%s energy_heating=%s energy_dhw=%s",
                self.ski,
                data["power_watts"],
                data["energy_consumed_kwh"],
                data["energy_consumed_heating_kwh"],
                data["energy_consumed_dhw_kwh"],
            )

            if self._was_unavailable:
                _LOGGER.info("EEBUS bridge connection restored at %s:%s", self.host, self.port)
                self._was_unavailable = False

            return data
        except grpc.aio.AioRpcError as err:
            if self._channel is not None:
                await self._channel.close()
                self._channel = None
            self._not_found_streak = 0

            if not self._was_unavailable:
                _LOGGER.warning(
                    "EEBUS bridge unavailable at %s:%s: %s", self.host, self.port, err
                )
                self._was_unavailable = True

            raise UpdateFailed(f"gRPC error: {err}") from err

    async def _async_register_remote_ski(
        self, device_stub: Any, force: bool
    ) -> None:
        """Register remote SKI with bridge, optionally forcing re-registration."""
        from . import proto_stubs
        try:
            await device_stub.RegisterRemoteSKI(
                proto_stubs.RegisterSKIRequest(ski=self.ski), timeout=RPC_TIMEOUT
            )
            self._ski_registered = True
            _LOGGER.info(
                "%s remote SKI %s with bridge",
                "Forced re-registration of" if force else "Registered",
                self.ski,
            )
        except grpc.aio.AioRpcError as err:
            if force:
                _LOGGER.warning(
                    "Forced remote SKI re-registration failed for %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )
            else:
                # Retry in next polling cycle until the bridge accepts registration.
                _LOGGER.debug(
                    "Remote SKI registration pending for %s: %s",
                    self.ski,
                    _rpc_error_text(err),
                )

    @staticmethod
    def _extract_scoped_energy_kwh(measurements: list[Any]) -> dict[str, float | None]:
        """Extract Vaillant/EEBUS scoped counters for heating and domestic hot water."""
        result: dict[str, float | None] = {"heating": None, "dhw": None}
        for measurement in measurements:
            measurement_type = str(getattr(measurement, "type", "")).lower().strip()
            if not measurement_type:
                continue
            normalized = measurement_type.replace("-", "_").replace(" ", "_")
            value = getattr(measurement, "value", None)
            if value is None:
                continue

            # Vaillant uses separate thermal storage contexts for heating and DHW.
            if (
                "energy" in normalized
                and ("domestic_hot_water" in normalized or "hot_water" in normalized or "dhw" in normalized)
            ):
                result["dhw"] = value
                continue

            if "energy" in normalized and ("heating" in normalized or "space_heating" in normalized):
                result["heating"] = value

        return result

    @staticmethod
    def _extract_standard_measurements(measurements: list[Any]) -> dict[str, float | None]:
        """Extract bridge-standardized measurement entries into coordinator keys."""
        result: dict[str, float | None] = {
            "power_watts": None,
            "energy_consumed_kwh": None,
            "energy_produced_kwh": None,
            "grid_frequency_hz": None,
            "power_l1_watts": None,
            "power_l2_watts": None,
            "power_l3_watts": None,
            "current_l1_ampere": None,
            "current_l2_ampere": None,
            "current_l3_ampere": None,
            "voltage_l1_volt": None,
            "voltage_l2_volt": None,
            "voltage_l3_volt": None,
        }
        key_map = {
            "power_consumption": "power_watts",
            "energy_consumed": "energy_consumed_kwh",
            "energy_produced": "energy_produced_kwh",
            "frequency": "grid_frequency_hz",
            "power_l1": "power_l1_watts",
            "power_l2": "power_l2_watts",
            "power_l3": "power_l3_watts",
            "current_l1": "current_l1_ampere",
            "current_l2": "current_l2_ampere",
            "current_l3": "current_l3_ampere",
            "voltage_l1": "voltage_l1_volt",
            "voltage_l2": "voltage_l2_volt",
            "voltage_l3": "voltage_l3_volt",
        }
        for measurement in measurements:
            measurement_type = str(getattr(measurement, "type", "")).strip().lower()
            if not measurement_type:
                continue
            normalized = measurement_type.replace("-", "_").replace(" ", "_")
            target_key = key_map.get(normalized)
            if target_key is None:
                continue
            value = getattr(measurement, "value", None)
            if value is None:
                continue
            result[target_key] = value
        return result

    async def async_write_lpc_limit(self, value_watts: float) -> None:
        """Write LPC consumption limit via gRPC."""
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        # Set optimistic state BEFORE gRPC calls so any concurrent coordinator
        # poll sees the protected value immediately during the round-trip.
        prev_limit = self.data.get("consumption_limit") if self.data else None
        self._last_lpc_write = time.monotonic()
        if self.data is not None:
            if self.data.get("consumption_limit") is None:
                self.data["consumption_limit"] = {}
            self.data["consumption_limit"]["value_watts"] = value_watts
            self.data["consumption_limit"]["is_active"] = True
        try:
            await stub.WriteConsumptionLimit(
                proto_stubs.WriteLoadLimitRequest(
                    ski=self.ski, value_watts=value_watts, is_active=True,
                    duration_seconds=self.lpc_duration_seconds,
                ),
                timeout=RPC_TIMEOUT,
            )
            self._lpc_supported = True
        except grpc.aio.AioRpcError as err:
            # Revert optimistic state so the next poll reflects reality.
            self._last_lpc_write = 0.0
            if self.data is not None:
                self.data["consumption_limit"] = prev_limit
            if _is_unimplemented(err):
                self._lpc_supported = False
                _LOGGER.info(
                    "LPC write unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_write_failsafe_limit(self, value_watts: float) -> None:
        """Write failsafe limit watts via gRPC."""
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        try:
            await stub.WriteFailsafeLimit(
                proto_stubs.WriteFailsafeLimitRequest(
                    ski=self.ski,
                    value_watts=value_watts,
                    duration_minimum_seconds=self.failsafe_duration_minimum_seconds,
                ),
                timeout=RPC_TIMEOUT,
            )
            self._failsafe_supported = True
        except grpc.aio.AioRpcError as err:
            if _is_unimplemented(err):
                self._failsafe_supported = False
                _LOGGER.info(
                    "Failsafe write unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_write_lpc_duration(self, duration_seconds: int) -> None:
        """Persist LPC limit duration (seconds). Applied on next WriteConsumptionLimit."""
        self.lpc_duration_seconds = max(60, duration_seconds)

    async def async_write_failsafe_duration(self, duration_minimum_seconds: int) -> None:
        """Persist failsafe minimum duration (seconds) and write to device."""
        # EEBUS spec mandates 2 h – 24 h.
        clamped = max(7200, min(86400, duration_minimum_seconds))
        self.failsafe_duration_minimum_seconds = clamped
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        # Read current failsafe watts so we can send both fields together.
        try:
            current = await stub.GetFailsafeLimit(
                proto_stubs.DeviceRequest(ski=self.ski), timeout=RPC_TIMEOUT
            )
            value_watts = current.value_watts
        except grpc.aio.AioRpcError:
            value_watts = 0.0
        try:
            await stub.WriteFailsafeLimit(
                proto_stubs.WriteFailsafeLimitRequest(
                    ski=self.ski,
                    value_watts=value_watts,
                    duration_minimum_seconds=clamped,
                ),
                timeout=RPC_TIMEOUT,
            )
            self._failsafe_supported = True
            if self.data and self.data.get("failsafe_limit") is not None:
                self.data["failsafe_limit"]["duration_minimum_seconds"] = clamped
        except grpc.aio.AioRpcError as err:
            if _is_unimplemented(err):
                self._failsafe_supported = False
                _LOGGER.info(
                    "Failsafe duration write unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_set_lpc_active(self, active: bool) -> None:
        """Activate or deactivate LPC limit via gRPC."""
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        # Optimistically update is_active BEFORE any gRPC call so concurrent
        # coordinator polls see the protected state during both round-trips.
        prev_active = (
            self.data["consumption_limit"].get("is_active")
            if self.data and self.data.get("consumption_limit") is not None
            else None
        )
        self._last_lpc_write = time.monotonic()
        if self.data and self.data.get("consumption_limit") is not None:
            self.data["consumption_limit"]["is_active"] = active
        # Always use a fixed duration of 3600s.
        # The device returns a countdown (remaining time) for duration_seconds,
        # not the configured value — so we must never reuse it as input.
        try:
            current = await stub.GetConsumptionLimit(
                proto_stubs.DeviceRequest(ski=self.ski), timeout=RPC_TIMEOUT
            )
            await stub.WriteConsumptionLimit(
                proto_stubs.WriteLoadLimitRequest(
                    ski=self.ski,
                    value_watts=current.value_watts,
                    is_active=active,
                    duration_seconds=self.lpc_duration_seconds,
                ),
                timeout=RPC_TIMEOUT,
            )
            self._lpc_supported = True
        except grpc.aio.AioRpcError as err:
            # Revert optimistic state so the next poll reflects reality.
            self._last_lpc_write = 0.0
            if self.data and self.data.get("consumption_limit") is not None:
                self.data["consumption_limit"]["is_active"] = prev_active
            if _is_unimplemented(err):
                self._lpc_supported = False
                _LOGGER.info(
                    "LPC activation unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_start_heartbeat(self) -> None:
        """Start EEBUS heartbeat via gRPC."""
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        try:
            await stub.StartHeartbeat(
                proto_stubs.DeviceRequest(ski=self.ski), timeout=RPC_TIMEOUT
            )
        except grpc.aio.AioRpcError as err:
            if _is_unimplemented(err):
                self._heartbeat_supported = False
                _LOGGER.info(
                    "Heartbeat start unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_stop_heartbeat(self) -> None:
        """Stop EEBUS heartbeat via gRPC."""
        channel = self._ensure_channel()
        from . import proto_stubs
        stub = proto_stubs.LPCServiceStub(channel)
        try:
            await stub.StopHeartbeat(
                proto_stubs.DeviceRequest(ski=self.ski), timeout=RPC_TIMEOUT
            )
        except grpc.aio.AioRpcError as err:
            if _is_unimplemented(err):
                self._heartbeat_supported = False
                _LOGGER.info(
                    "Heartbeat stop unsupported for SKI %s: %s", self.ski, err.details()
                )
                return
            raise

    async def async_shutdown(self) -> None:
        """Close gRPC channel and cancel stream tasks."""
        for task in self._stream_tasks:
            task.cancel()
        self._stream_tasks.clear()
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    # ------------------------------------------------------------------
    # EMS-ESP integration — SG-Ready control via pvmaxcomp
    # ------------------------------------------------------------------
    # SG-Ready mapping (Bosch Compress 5800i via EMS-ESP REST API):
    #   Mode 2 Normal      → pvmaxcomp = 0   (WP runs by own logic)
    #   Mode 3 Encourage   → pvmaxcomp = 15  (moderate PV surplus hint)
    #   Mode 4 Force       → pvmaxcomp = 25  (max compressor + DHW one-time)
    #
    # The Bosch factory/idle state reports pvmaxcomp ≈ 1.5 (not 0).
    # Read-back thresholds therefore use margins around the written values:
    #   "normal"   if pvmaxcomp < 5    (covers 0 and factory default 1.5)
    #   "encourage" if 5 ≤ pvmaxcomp < 20
    #   "force"    if pvmaxcomp ≥ 20
    #
    # EMS-ESP REST API: POST http://<host>/api/boiler
    #   body: {"cmd": "pvmaxcomp", "value": <float>}
    # No authentication needed (notoken_api = true).
    # ------------------------------------------------------------------

    def set_emsesp_url(self, url: str) -> None:
        """Set the EMS-ESP base URL (e.g. 'http://ems-esp')."""
        self._emsesp_url = url.rstrip("/") if url else ""

    @property
    def emsesp_url(self) -> str:
        """Return the configured EMS-ESP base URL."""
        return self._emsesp_url

    async def _emsesp_post(self, device: str, cmd: str, value: Any) -> None:
        """POST a command to the EMS-ESP REST API."""
        url = f"{self._emsesp_url}/api/{device}"
        payload = {"cmd": cmd, "value": value}
        session = async_get_clientsession(self.hass)
        try:
            async with session.post(
                url, json=payload, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status not in (200, 204):
                    text = await resp.text()
                    _LOGGER.warning(
                        "EMS-ESP POST %s %s=%s returned HTTP %s: %s",
                        url, cmd, value, resp.status, text[:200],
                    )
                else:
                    _LOGGER.debug(
                        "EMS-ESP POST %s %s=%s → HTTP %s", url, cmd, value, resp.status
                    )
        except Exception as err:
            _LOGGER.debug("EMS-ESP POST %s %s=%s failed: %s", url, cmd, value, err)
            raise

    async def async_set_sg_ready_mode(self, mode: str) -> None:
        """Set SG-Ready mode via EMS-ESP thermostat controls.

        Mapping to CS5800i / Rego 3000 thermostat commands:
          normal   (Mode 2): boost=0, hc1/seltemp restored to base value
          encourage (Mode 3): boost=0, hc1/seltemp raised by SG_READY_SELTEMP_OFFSET_K
          force    (Mode 4): boost=1  (max compressor, thermostat manages seltemp)

        mode: "normal" | "encourage" | "force"
        """
        if not self._emsesp_url:
            raise ValueError("EMS-ESP URL not configured; set it via integration options")

        if mode not in _SG_READY_MODES:
            raise ValueError(f"Invalid SG-Ready mode: {mode!r}. Must be one of {sorted(_SG_READY_MODES)}")

        # Stamp before any awaits so concurrent polls see the protection window.
        self._last_sg_ready_write = time.monotonic()
        # Optimistic update so the entity reflects the new state immediately.
        if self.data is not None:
            self.data["sg_ready_mode"] = mode

        if mode == "force":
            # Maximise both thermal stores:
            #   hc1/boost  → heating circuit runs at max for boosttime hours
            #   dhw/onetime → one-time DHW charge (stored in the boiler's hot-water tank)
            await self._emsesp_post("thermostat", "hc1/boost", 1)
            try:
                await self._emsesp_post("boiler", "dhw/onetime", 1)
            except Exception:
                _LOGGER.warning("SG-Ready force: DHW one-time charge trigger failed (non-fatal)")

        elif mode == "encourage":
            # Save base seltemp if not already stored (e.g. first use after restart).
            if self._sg_ready_base_seltemp is None:
                seltemp = await self._async_read_emsesp_seltemp()
                if seltemp is not None:
                    self._sg_ready_base_seltemp = seltemp
                else:
                    _LOGGER.warning(
                        "SG-Ready encourage: could not read current seltemp from EMS-ESP; "
                        "using fallback base of 21 °C"
                    )
                    self._sg_ready_base_seltemp = 21.0
            target = self._sg_ready_base_seltemp + SG_READY_SELTEMP_OFFSET_K
            await self._emsesp_post("thermostat", "hc1/seltemp", target)
            # Make sure boost is off (in case we're coming from force).
            await self._emsesp_post("thermostat", "hc1/boost", 0)

        else:  # normal
            await self._emsesp_post("thermostat", "hc1/boost", 0)
            if self._sg_ready_base_seltemp is not None:
                await self._emsesp_post("thermostat", "hc1/seltemp", self._sg_ready_base_seltemp)

        _LOGGER.debug("SG-Ready mode set to %r (base_seltemp=%s)", mode, self._sg_ready_base_seltemp)

    async def _async_read_emsesp_hc1(self) -> dict | None:
        """Read thermostat hc1 data from EMS-ESP (boost, seltemp, ...)."""
        if not self._emsesp_url:
            return None
        url = f"{self._emsesp_url}/api/thermostat/hc1"
        session = async_get_clientsession(self.hass)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict):
                        return data
        except Exception as err:
            _LOGGER.debug("EMS-ESP GET thermostat/hc1 failed: %s", err)
        return None

    async def _async_read_emsesp_seltemp(self) -> float | None:
        """Read the current hc1/seltemp from EMS-ESP."""
        if not self._emsesp_url:
            return None
        url = f"{self._emsesp_url}/api/thermostat/hc1/seltemp"
        session = async_get_clientsession(self.hass)
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json(content_type=None)
                    if isinstance(data, dict):
                        return float(data.get("api_data", data.get("value", 0)))
                    return float(data)
        except Exception as err:
            _LOGGER.debug("EMS-ESP GET thermostat/hc1/seltemp failed: %s", err)
        return None
