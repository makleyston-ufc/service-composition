# bus producer

## Description

Publishes bus telemetry observations to MQTT using records from a local JSON dataset and IoT-Stream/SOSA formatted payloads.

## Dataset Read

- `dataset/tempo_real_convencional_json_070326052133.json`
- Optional reference file in the same folder: `dataset/dicionario_arquivo_convencional.csv`

## Published Topics

- Default publish topic: `transport/bus/telemetry`
- Configurable by environment variable: `MQTT_PUB_TOPIC`
