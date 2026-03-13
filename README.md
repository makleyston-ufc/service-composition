# Service Composition

Service Composition is an urban analytics project with MQTT data producers (C++), a Mosquitto broker, and AI/ML inference services (Python) organized in composition levels (L0 to L3).

## Scenario Illustration (`scenarios.dot`)

The project scenario is defined in [`scenarios.dot`](scenarios.dot) and rendered below as an image.

![Service composition scenario](docs/scenarios.png)

## Directory Overview

### Producers

- `producer/bus`: bus telemetry producer.
- `producer/radar`: radar telemetry producer.

### Services

- `services/L0`: base inference services from raw observations.
- `services/L1`: first-level composition services.
- `services/L2`: intermediate composition services.
- `services/L3`: strategic composition services.
- `services/ufcity_microservice_lib`: shared runtime, MQTT, and semantic utilities.

### Messaging Infrastructure

- `broker-mqtt/mosquitto`: Mosquitto configuration and runtime data.

## Service Tree

- `services/L0`
- `services/L0/traffic-flow`
- `services/L0/traffic-speed`
- `services/L0/bus-operation-status`
- `services/L0/bus-regional-supply`
- `services/L1`
- `services/L1/mobility-efficiency-index`
- `services/L1/urban-congestion-index`
- `services/L1/regional-mobility-pressure`
- `services/L1/alternative-regional-pressure`
- `services/L2`
- `services/L2/social-accessibility-index`
- `services/L2/urban-risk-index`
- `services/L3`
- `services/L3/strategic-intervention-priority`
- `services/L3/integrated-urban-vulnerability`

## Producers Tree

- `producer/bus`
- `producer/radar`
