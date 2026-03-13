# radar producer

## Description

Publishes radar telemetry observations to MQTT by reading hourly JSON files and serializing them as IoT-Stream/SOSA observation messages.

## Dataset Read

- `dataset/20230901/*.json`

## Published Topics

- Default publish topic: `transport/radar/telemetry`
- Configurable by environment variable: `MQTT_PUB_TOPIC`
