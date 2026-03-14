from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

SERVICE_FILE = Path(__file__).resolve()
IMPORT_ROOT_CANDIDATES = [SERVICE_FILE.parents[2], SERVICE_FILE.parents[1]]
for candidate in IMPORT_ROOT_CANDIDATES:
    if (candidate / "ufcity_microservice_lib").exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))
        break

from ufcity_microservice_lib import MqttClient, MqttPublish, Observer

SERVICE_NAME = "mobility-efficiency-index"
DEFAULT_INPUT_TOPICS = [
    "service/mobility/inference/traffic-speed",
    "service/mobility/inference/bus-operation-status",
]
DEFAULT_OUTPUT_TOPIC = "service/mobility/inference/mobility-efficiency-index"
DEFAULT_SERVICE_URN = "urn:ufcity:service:mobility-efficiency-index"
DEFAULT_PROPERTY_KIND = "urn:ufcity:propertykind:MobilityEfficiencyIndex"
MODEL_PATH = SERVICE_FILE.parent / "model.joblib"


def _parse_topics(raw_topics: str | None, defaults: list[str]) -> list[str]:
    if not raw_topics:
        return defaults
    parsed = [topic.strip() for topic in raw_topics.split(",") if topic.strip()]
    return parsed or defaults


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extract_timestamp(message: dict[str, Any]) -> str:
    result_time = message.get("sosa:resultTime")
    if isinstance(result_time, dict):
        value = result_time.get("@value")
        if isinstance(value, str) and value:
            return value
    if isinstance(result_time, str) and result_time:
        return result_time

    has_ts = message.get("saref:hasTimestamp")
    if isinstance(has_ts, str) and has_ts:
        return has_ts

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _extract_result_block(message: dict[str, Any]) -> dict[str, Any]:
    result = message.get("saref:hasResult")
    if isinstance(result, dict):
        return result
    simple_result = message.get("sosa:hasSimpleResult")
    if isinstance(simple_result, dict):
        return simple_result
    return {}


def _extract_id(text: Any, pattern: str) -> float | None:
    if not isinstance(text, str) or not text:
        return None
    match = re.search(pattern, text)
    if not match:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _parse_ts_to_hour_weekday(ts: Any) -> tuple[float | None, float | None]:
    if not isinstance(ts, str) or not ts:
        return None, None
    parsed = pd.to_datetime(ts, errors="coerce")
    if pd.isna(parsed):
        return None, None
    return float(parsed.hour), float(parsed.dayofweek)


class MobilityEfficiencyIndexService(Observer):
    def __init__(
        self,
        *,
        model_artifact: dict[str, Any],
        publisher: MqttPublish,
        input_topics: list[str],
        output_topic: str,
        service_urn: str,
        property_kind: str,
    ):
        self.pipeline = model_artifact["pipeline"]
        self.features = model_artifact["features"]
        self.index_weights = model_artifact.get("index_weights", {"baixa": 0.0, "media": 50.0, "alta": 100.0})
        self.publisher = publisher
        self.input_topics = input_topics
        self.expected_topics = set(input_topics)
        self.output_topic = output_topic
        self.service_urn = service_urn
        self.property_kind = property_kind
        self.latest_by_topic: dict[str, dict[str, Any]] = {}

    def _parse_traffic_input(self, message: dict[str, Any]) -> dict[str, Any]:
        result = _extract_result_block(message)
        ts = message.get("saref:hasTimestamp")
        hour, weekday = _parse_ts_to_hour_weekday(ts)

        return {
            "traffic_status": result.get("saref:hasValue"),
            "traffic_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "traffic_segment_id": _extract_id(message.get("saref:hasFeatureOfInterest"), r"road-segment:(\d+)"),
            "traffic_hour": hour,
            "traffic_weekday": weekday,
        }

    def _parse_bus_input(self, message: dict[str, Any]) -> dict[str, Any]:
        result = _extract_result_block(message)
        ts = message.get("saref:hasTimestamp")
        hour, weekday = _parse_ts_to_hour_weekday(ts)

        return {
            "bus_status": result.get("saref:hasValue"),
            "bus_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "bus_vehicle_id": _extract_id(message.get("saref:hasFeatureOfInterest"), r"bus:vehicle:(\d+)"),
            "bus_hour": hour,
            "bus_weekday": weekday,
        }

    def _build_model_input(self) -> pd.DataFrame:
        traffic_topic = self.input_topics[0]
        bus_topic = self.input_topics[1]
        traffic_features = self.latest_by_topic[traffic_topic]["features"]
        bus_features = self.latest_by_topic[bus_topic]["features"]

        row = {**traffic_features, **bus_features}
        return pd.DataFrame([row], columns=self.features)

    def _compute_efficiency_index(self, proba: np.ndarray, classes: list[str]) -> float:
        weights = np.array([float(self.index_weights.get(cls, 50.0)) for cls in classes], dtype=float)
        value = float(np.dot(proba, weights))
        return float(np.clip(value, 0.0, 100.0))

    def _build_output_message(
        self,
        *,
        prediction: str,
        confidence: float | None,
        efficiency_index: float | None,
        class_probabilities: dict[str, float] | None,
    ) -> dict[str, Any]:
        input_ids = []
        timestamps = []
        for input_topic in self.input_topics:
            input_item = self.latest_by_topic[input_topic]
            obs_id = input_item.get("observation_id")
            if obs_id:
                input_ids.append(str(obs_id))
            timestamps.append(str(input_item["timestamp"]))

        timestamp = max(timestamps) if timestamps else datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        output_id = f"{self.service_urn}:obs:{timestamp}"

        payload: dict[str, Any] = {
            "inputs_summary": {
                "traffic_status": self.latest_by_topic[self.input_topics[0]]["features"].get("traffic_status"),
                "bus_status": self.latest_by_topic[self.input_topics[1]]["features"].get("bus_status"),
            }
        }
        if efficiency_index is not None:
            payload["efficiency_index"] = round(efficiency_index, 3)
        if class_probabilities:
            payload["class_probabilities"] = class_probabilities

        message: dict[str, Any] = {
            "@context": {
                "iot-stream": "http://purl.org/iot/ontology/iot-stream#",
                "ssn": "http://www.w3.org/ns/ssn/",
                "saref": "https://saref.etsi.org/core/",
                "sosa": "http://www.w3.org/ns/sosa/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": output_id,
            "@type": [
                "iot-stream:StreamObservation",
                "saref:Observation",
            ],
            "iot-stream:belongsTo": self.service_urn,
            "iot-stream:derivedFrom": {
                "@type": "iot-stream:Analytics",
                "ssn:hasInput": input_ids,
            },
            "saref:hasPropertyOfInterest": {
                "@type": "saref:PropertyOfInterest",
                "saref:hasPropertyKind": self.property_kind,
            },
            "saref:hasResult": {
                "@type": "saref:PropertyValue",
                "saref:hasValue": prediction,
            },
            "saref:hasTimestamp": timestamp,
            "sosa:resultTime": {
                "@type": "xsd:dateTime",
                "@value": timestamp,
            },
            "urn:ufcity:payload": payload,
        }

        if confidence is not None:
            message["saref:hasResult"]["urn:ufcity:confidence"] = round(float(confidence), 6)
        if efficiency_index is not None:
            message["saref:hasResult"]["urn:ufcity:efficiencyIndex"] = round(float(efficiency_index), 3)

        return message

    def update(self, topic: str, message: Any) -> None:
        if not isinstance(message, dict):
            return

        if topic not in self.expected_topics:
            return

        if topic == self.input_topics[0]:
            parsed_features = self._parse_traffic_input(message)
        elif topic == self.input_topics[1]:
            parsed_features = self._parse_bus_input(message)
        else:
            return

        self.latest_by_topic[topic] = {
            "features": parsed_features,
            "observation_id": message.get("@id"),
            "timestamp": _extract_timestamp(message),
        }

        if not self.expected_topics.issubset(self.latest_by_topic.keys()):
            return

        model_input = self._build_model_input()
        prediction = str(self.pipeline.predict(model_input)[0])

        confidence = None
        efficiency_index = None
        class_probabilities = None
        if hasattr(self.pipeline, "predict_proba"):
            proba = self.pipeline.predict_proba(model_input)[0]
            classes = [str(c) for c in self.pipeline.named_steps["model"].classes_]
            confidence = float(np.max(proba))
            efficiency_index = self._compute_efficiency_index(proba=proba, classes=classes)
            class_probabilities = {cls: round(float(p), 6) for cls, p in zip(classes, proba)}

        output_message = self._build_output_message(
            prediction=prediction,
            confidence=confidence,
            efficiency_index=efficiency_index,
            class_probabilities=class_probabilities,
        )
        self.publisher.publish_single(self.output_topic, output_message)
        print(
            f"[{SERVICE_NAME}] publicado eficiencia={prediction} "
            f"index={None if efficiency_index is None else round(efficiency_index, 3)} "
            f"topic={self.output_topic}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo nao encontrado em {MODEL_PATH}")

    sub_topics_env = os.getenv("MQTT_SUB_TOPICS", os.getenv("MQTT_SUB_TOPIC"))
    input_topics = _parse_topics(sub_topics_env, DEFAULT_INPUT_TOPICS)
    if len(input_topics) != 2:
        raise ValueError(
            "Este servico exige exatamente 2 topicos de entrada, na ordem: "
            "traffic-speed,bus-operation-status"
        )

    output_topic = os.getenv("MQTT_PUB_TOPIC", DEFAULT_OUTPUT_TOPIC)
    service_urn = os.getenv("SERVICE_URN", DEFAULT_SERVICE_URN)
    property_kind = os.getenv("SEMANTIC_PROPERTY_KIND", DEFAULT_PROPERTY_KIND)
    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_client_id = os.getenv("MQTT_CLIENT_ID", SERVICE_NAME)

    model_artifact = joblib.load(MODEL_PATH)
    if not isinstance(model_artifact, dict) or "pipeline" not in model_artifact or "features" not in model_artifact:
        raise ValueError("Artifact de modelo invalido: esperado dict com 'pipeline' e 'features'")

    publisher = MqttPublish({"broker_address": mqtt_host, "port": mqtt_port})
    observer = MobilityEfficiencyIndexService(
        model_artifact=model_artifact,
        publisher=publisher,
        input_topics=input_topics,
        output_topic=output_topic,
        service_urn=service_urn,
        property_kind=property_kind,
    )

    client = MqttClient(
        {
            "broker_address": mqtt_host,
            "port": mqtt_port,
            "client_id": mqtt_client_id,
            "topics": input_topics,
            "qos": 0,
        }
    )
    client.attach(observer)
    print(
        f"[{SERVICE_NAME}] iniciando servico com "
        f"sub={input_topics} pub={output_topic} broker={mqtt_host}:{mqtt_port}"
    )
    client.subscribe_to_topics()


if __name__ == "__main__":
    main()
