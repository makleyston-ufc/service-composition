from __future__ import annotations

import os
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

SERVICE_NAME = "alternative-regional-pressure"
DEFAULT_INPUT_TOPICS = [
    "service/mobility/inference/traffic-flow",
    "service/mobility/inference/bus-regional-supply",
]
DEFAULT_OUTPUT_TOPIC = "service/mobility/inference/alternative-regional-pressure"
DEFAULT_SERVICE_URN = "urn:ufcity:service:alternative-regional-pressure"
DEFAULT_PROPERTY_KIND = "urn:ufcity:propertykind:AlternativeRegionalPressure"
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


def _safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
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


def _extract_payload_block(message: dict[str, Any]) -> dict[str, Any]:
    payload = message.get("urn:ufcity:payload")
    if isinstance(payload, dict):
        return payload
    simple_result = message.get("sosa:hasSimpleResult")
    if isinstance(simple_result, dict):
        return simple_result
    return {}


def _parse_hr_to_hour_weekday(hr_raw: Any) -> tuple[int | None, int | None]:
    hr = str(hr_raw) if hr_raw is not None else ""
    if len(hr) == 14 and hr.isdigit():
        dt = datetime.strptime(hr, "%Y%m%d%H%M%S")
        return dt.hour, dt.weekday()
    return None, None


class AlternativeRegionalPressureService(Observer):
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
        payload = _extract_payload_block(message)
        features = payload.get("features", {}) if isinstance(payload.get("features"), dict) else {}

        return {
            "traffic_level": result.get("saref:hasValue"),
            "traffic_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "traffic_direction": features.get("SENTIDO"),
            "traffic_lane": _safe_int(features.get("FAIXA")),
            "vehicle_count": _safe_float(features.get("vehicle_count")),
            "avg_speed": _safe_float(features.get("avg_speed")),
            "speed_std": _safe_float(features.get("speed_std")),
            "pct_moto": _safe_float(features.get("pct_moto")),
            "pct_heavy": _safe_float(features.get("pct_heavy")),
            "traffic_hour": _safe_int(features.get("hour")),
            "traffic_weekday": _safe_int(features.get("weekday")),
        }

    def _parse_bus_supply_input(self, message: dict[str, Any]) -> dict[str, Any]:
        result = _extract_result_block(message)
        payload = _extract_payload_block(message)
        hour, weekday = _parse_hr_to_hour_weekday(payload.get("HR"))

        return {
            "bus_supply_level": result.get("saref:hasValue"),
            "bus_supply_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "bus_lt": _safe_float(payload.get("LT")),
            "bus_lg": _safe_float(payload.get("LG")),
            "bus_vl": _safe_float(payload.get("VL")),
            "bus_nl": _safe_float(payload.get("NL")),
            "bus_dg": _safe_float(payload.get("DG")),
            "bus_sv": _safe_float(payload.get("SV")),
            "bus_dt": _safe_float(payload.get("DT")),
            "bus_hour": _safe_int(hour),
            "bus_weekday": _safe_int(weekday),
        }

    def _build_model_input(self) -> pd.DataFrame:
        traffic_topic = self.input_topics[0]
        bus_topic = self.input_topics[1]
        traffic_features = self.latest_by_topic[traffic_topic]["features"]
        bus_features = self.latest_by_topic[bus_topic]["features"]

        row = {**traffic_features, **bus_features}
        return pd.DataFrame([row], columns=self.features)

    def _compute_pressure_index(self, proba: np.ndarray, classes: list[str]) -> float:
        weights = np.array([float(self.index_weights.get(cls, 50.0)) for cls in classes], dtype=float)
        value = float(np.dot(proba, weights))
        return float(np.clip(value, 0.0, 100.0))

    def _build_output_message(
        self,
        *,
        prediction: str,
        confidence: float | None,
        pressure_index: float | None,
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
                "traffic_level": self.latest_by_topic[self.input_topics[0]]["features"].get("traffic_level"),
                "bus_supply_level": self.latest_by_topic[self.input_topics[1]]["features"].get("bus_supply_level"),
            }
        }
        if pressure_index is not None:
            payload["pressure_index"] = round(pressure_index, 3)
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
        if pressure_index is not None:
            message["saref:hasResult"]["urn:ufcity:pressureIndex"] = round(float(pressure_index), 3)

        return message

    def update(self, topic: str, message: Any) -> None:
        if not isinstance(message, dict):
            return

        if topic not in self.expected_topics:
            return

        if topic == self.input_topics[0]:
            parsed_features = self._parse_traffic_input(message)
        elif topic == self.input_topics[1]:
            parsed_features = self._parse_bus_supply_input(message)
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
        pressure_index = None
        class_probabilities = None
        if hasattr(self.pipeline, "predict_proba"):
            proba = self.pipeline.predict_proba(model_input)[0]
            classes = [str(c) for c in self.pipeline.named_steps["model"].classes_]
            confidence = float(np.max(proba))
            pressure_index = self._compute_pressure_index(proba=proba, classes=classes)
            class_probabilities = {cls: round(float(p), 6) for cls, p in zip(classes, proba)}

        output_message = self._build_output_message(
            prediction=prediction,
            confidence=confidence,
            pressure_index=pressure_index,
            class_probabilities=class_probabilities,
        )
        self.publisher.publish_single(self.output_topic, output_message)
        print(
            f"[{SERVICE_NAME}] publicado pressure={prediction} "
            f"index={None if pressure_index is None else round(pressure_index, 3)} "
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
            "traffic-flow,bus-regional-supply"
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
    observer = AlternativeRegionalPressureService(
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
