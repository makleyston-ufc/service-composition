from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

import paho.mqtt.client as mqtt


class Observer(ABC):
    @abstractmethod
    def update(self, topic: str, message: Any) -> None:
        raise NotImplementedError


class MqttClient:
    def __init__(self, configs: dict[str, Any]):
        self.broker_address = configs.get("broker_address", "localhost")
        self.port = int(configs.get("port", 1883))
        self.keepalive = int(configs.get("keepalive", 30))
        self.client_id = configs.get("client_id", "")
        self.qos = int(configs.get("qos", 0))
        self.topics = list(configs.get("topics", [])) or ["#"]
        self._observers: list[Observer] = []

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.client_id,
        )
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.broker_address, self.port, self.keepalive)

    def attach(self, observer: Observer) -> None:
        if observer not in self._observers:
            self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        if observer in self._observers:
            self._observers.remove(observer)

    def notify(self, topic: str, message: Any) -> None:
        for observer in self._observers:
            observer.update(topic, message)

    def on_connect(self, client, userdata, flags, reason_code, properties=None):
        print(f"[mqtt] connected with rc={reason_code}")
        for topic in self.topics:
            client.subscribe(topic, qos=self.qos)
            print(f"[mqtt] subscribed to {topic} (qos={self.qos})")

    def on_message(self, client, userdata, message):
        payload_text = message.payload.decode("utf-8", errors="ignore")
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            payload = payload_text
        self.notify(message.topic, payload)

    def set_topics(self, topics: list[str]) -> "MqttClient":
        self.topics = topics
        return self

    def set_qos(self, qos: int) -> "MqttClient":
        self.qos = qos
        return self

    def subscribe_to_topics(self) -> None:
        self.client.loop_forever()

    def start_background(self) -> None:
        self.client.loop_start()

    def stop(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()
