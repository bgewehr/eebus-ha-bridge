"""Constants for the EEBUS integration."""

from homeassistant.const import Platform

DOMAIN = "eebus"
DEFAULT_GRPC_PORT = 50051
CONF_GRPC_HOST = "grpc_host"
CONF_GRPC_PORT = "grpc_port"
CONF_DEVICE_SKI = "device_ski"
CONF_EMSESP_URL = "emsesp_url"

PLATFORMS = [
    Platform.BINARY_SENSOR,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.SENSOR,
    Platform.SWITCH,
]

PARALLEL_UPDATES = 0  # Coordinator-based, no per-entity polling
