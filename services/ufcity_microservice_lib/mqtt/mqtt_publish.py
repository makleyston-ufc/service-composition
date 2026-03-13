from __future__ import annotations

import json
from typing import Any

import paho.mqtt.publish as publish


class MqttPublish:
    def __init__(self, configs: dict[str, Any]):
        self.broker_address = configs.get("broker_address", "localhost")
        self.port = int(configs.get("port", 1883))

    def _serialize(self, message: Any) -> str:
        if isinstance(message, str):
            return message
        return json.dumps(message, ensure_ascii=False)

    def publish_multiple(self, messages: list[dict[str, Any]]) -> None:
        publish.multiple(messages, hostname=self.broker_address, port=self.port)

    def publish_single(self, topic: str, message: Any, qos: int = 0, retain: bool = False) -> None:
        publish.single(
            topic=topic,
            payload=self._serialize(message),
            qos=qos,
            retain=retain,
            hostname=self.broker_address,
            port=self.port,
        )
