from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

from ..mqtt import MqttClient, MqttPublish, Observer


@dataclass
class ServiceConfig:
    service_name: str
    service_urn: str
    input_topics: list[str]
    output_topic: str
    semantic_property_kind: str
    model_path: Path
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_client_id: str = ""


def _extract_payload(message: Any) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {}
    simple_result = message.get("sosa:hasSimpleResult")
    if isinstance(simple_result, dict):
        return simple_result
    return message


def _extract_observation_id(message: Any) -> str | None:
    if isinstance(message, dict):
        obs_id = message.get("@id")
        if isinstance(obs_id, str):
            return obs_id
    return None


def _extract_timestamp(message: Any) -> str:
    if isinstance(message, dict):
        result_time = message.get("sosa:resultTime")
        if isinstance(result_time, dict):
            value = result_time.get("@value")
            if isinstance(value, str) and value:
                return value
        if isinstance(result_time, str) and result_time:
            return result_time
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class GenericInferenceService(Observer):
    def __init__(self, config: ServiceConfig, model_artifact: Any, publisher: MqttPublish):
        self.config = config
        self.model_artifact = model_artifact
        self.publisher = publisher
        self.expected_topics = set(config.input_topics)
        self.latest_by_topic: dict[str, dict[str, Any]] = {}

    def _predict(self, aggregated_payload: dict[str, Any]) -> tuple[str, float | None]:
        if self.model_artifact is None:
            return "model-not-loaded", None

        model = self.model_artifact
        if isinstance(model, dict) and "pipeline" in model:
            model = model["pipeline"]

        if hasattr(model, "predict"):
            try:
                prediction = model.predict([aggregated_payload])[0]
                confidence = None
                if hasattr(model, "predict_proba"):
                    probs = model.predict_proba([aggregated_payload])[0]
                    confidence = float(max(probs))
                return str(prediction), confidence
            except Exception:
                return "inference-error", None

        return "model-without-predict-method", None

    def _build_output(
        self,
        prediction: str,
        timestamp: str,
        input_observation_ids: list[str],
        confidence: float | None,
        aggregated_payload: dict[str, Any],
    ) -> dict[str, Any]:
        output: dict[str, Any] = {
            "@context": {
                "iot-stream": "http://purl.org/iot/ontology/iot-stream#",
                "ssn": "http://www.w3.org/ns/ssn/",
                "saref": "https://saref.etsi.org/core/",
            },
            "@id": f"{self.config.service_urn}:obs:{timestamp}",
            "@type": [
                "iot-stream:StreamObservation",
                "saref:Observation",
            ],
            "iot-stream:belongsTo": self.config.service_urn,
            "iot-stream:derivedFrom": {
                "@type": "iot-stream:Analytics",
                "ssn:hasInput": input_observation_ids,
            },
            "saref:hasPropertyOfInterest": {
                "@type": "saref:PropertyOfInterest",
                "saref:hasPropertyKind": self.config.semantic_property_kind,
            },
            "saref:hasResult": {
                "@type": "saref:PropertyValue",
                "saref:hasValue": prediction,
            },
            "saref:hasTimestamp": timestamp,
            "urn:ufcity:payload": aggregated_payload,
        }
        if confidence is not None:
            output["saref:hasResult"]["urn:ufcity:confidence"] = round(confidence, 6)
        return output

    def update(self, topic: str, message: Any) -> None:
        payload = _extract_payload(message)
        if not payload:
            return

        self.latest_by_topic[topic] = {
            "payload": payload,
            "observation_id": _extract_observation_id(message),
            "timestamp": _extract_timestamp(message),
        }

        if not self.expected_topics.issubset(self.latest_by_topic.keys()):
            return

        aggregated: dict[str, Any] = {}
        input_obs_ids: list[str] = []
        timestamps: list[str] = []

        for input_topic in self.config.input_topics:
            input_item = self.latest_by_topic[input_topic]
            topic_key = input_topic.replace("/", "__")
            aggregated[topic_key] = input_item["payload"]
            if input_item.get("observation_id"):
                input_obs_ids.append(str(input_item["observation_id"]))
            timestamps.append(str(input_item["timestamp"]))

        timestamp = max(timestamps) if timestamps else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        prediction, confidence = self._predict(aggregated)
        output_message = self._build_output(prediction, timestamp, input_obs_ids, confidence, aggregated)
        self.publisher.publish_single(self.config.output_topic, output_message)
        print(f"[{self.config.service_name}] published prediction={prediction} topic={self.config.output_topic}")


def _load_model(model_path: Path, service_name: str) -> Any:
    if model_path.exists():
        print(f"[{service_name}] loading model from {model_path}")
        return joblib.load(model_path)
    print(f"[{service_name}] model not found at {model_path}, running with placeholder prediction")
    return None


def run_generic_service(config: ServiceConfig) -> None:
    model_artifact = _load_model(config.model_path, config.service_name)
    publisher = MqttPublish({"broker_address": config.mqtt_host, "port": config.mqtt_port})
    observer = GenericInferenceService(config, model_artifact, publisher)

    client = MqttClient(
        {
            "broker_address": config.mqtt_host,
            "port": config.mqtt_port,
            "client_id": config.mqtt_client_id or config.service_name,
            "topics": config.input_topics,
            "qos": 0,
        }
    )
    client.attach(observer)
    print(f"[{config.service_name}] listening on {config.input_topics} and publishing to {config.output_topic}")
    client.subscribe_to_topics()
