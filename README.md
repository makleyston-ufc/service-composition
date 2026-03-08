# ServiceComposition

Estrutura inicial para um projeto com comunicação MQTT via Docker Compose.

## Estrutura criada

- `broker-mqtt/mosquitto/`: broker MQTT (Mosquitto).
- `producer/bus/`: producer em C++.
- `producer/radar/`: producer em C++.
- `services/L0/exemplo-servico-python/`: serviço Python de exemplo com pub/sub.
- `services/L1/`, `services/L2/`, `services/L3/`: níveis prontos para receber novos serviços.

## Subir o ambiente

```bash
docker compose up --build
```

Todos os serviços estão configurados para conectar no broker `mosquitto` e publicar/assinar tópicos MQTT.
