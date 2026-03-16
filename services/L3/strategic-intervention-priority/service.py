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

SERVICE_NAME = "strategic-intervention-priority"
DEFAULT_INPUT_TOPICS = [
    "service/social/inference/social-accessibility-index",
    "service/mobility/inference/regional-mobility-pressure",
]
DEFAULT_OUTPUT_TOPIC = "service/planning/inference/strategic-intervention-priority"
DEFAULT_SERVICE_URN = "urn:ufcity:service:strategic-intervention-priority"
DEFAULT_PROPERTY_KIND = "urn:ufcity:propertykind:StrategicInterventionPriority"
DEFAULT_MODEL_PATH = SERVICE_FILE.parent / "model.joblib"
DEFAULT_ZONING_PATH = SERVICE_FILE.parent / "dataset" / "20260101_zoneamento_11181.csv"

MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))
ZONING_PATH = Path(os.getenv("ZONING_PATH", str(DEFAULT_ZONING_PATH)))

DEFAULT_FEATURES = [
    "pressure_class",
    "pressure_confidence",
    "pressure_index",
    "traffic_level",
    "bus_supply_level",
    "pressure_prob_baixa",
    "pressure_prob_media",
    "pressure_prob_alta",
    "pressure_hour",
    "pressure_weekday",
    "social_class",
    "social_confidence",
    "social_accessibility_index",
    "mobility_class",
    "social_pressure_class",
    "social_traffic_status",
    "social_bus_status",
    "social_traffic_level",
    "social_bus_supply_level",
    "social_prob_baixa",
    "social_prob_media",
    "social_prob_alta",
    "social_hour",
    "social_weekday",
    "pct_social_interest",
    "pct_aeis",
    "pct_zeis",
    "zoning_vulnerability",
]


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


def _load_zoning_factor(path: Path) -> dict[str, float]:
    if not path.exists():
        return {
            "pct_social_interest": 0.0,
            "pct_aeis": 0.0,
            "pct_zeis": 0.0,
            "zoning_vulnerability": 0.0,
        }

    df = pd.read_csv(path, sep=";")
    sigla = df.get("SIGLA_TIPO_ZONEAMENTO", pd.Series(dtype=str)).astype(str).str.strip().str.upper()

    social_interest = {"AEIS_1", "AEIS_2", "ZEIS-1", "ZEIS-2"}
    pct_social_interest = float((sigla.isin(social_interest)).mean())
    pct_aeis = float((sigla.str.startswith("AEIS")).mean())
    pct_zeis = float((sigla.str.startswith("ZEIS")).mean())

    zoning_vulnerability = max(0.0, min(1.0, pct_social_interest))

    return {
        "pct_social_interest": pct_social_interest,
        "pct_aeis": pct_aeis,
        "pct_zeis": pct_zeis,
        "zoning_vulnerability": zoning_vulnerability,
    }


class StrategicInterventionPriorityService(Observer):
    def __init__(
        self,
        *,
        model_artifact: Any,
        publisher: MqttPublish,
        input_topics: list[str],
        output_topic: str,
        service_urn: str,
        property_kind: str,
        zoning_context: dict[str, float],
    ):
        if isinstance(model_artifact, dict):
            if "pipeline" in model_artifact:
                self.pipeline = model_artifact["pipeline"]
            elif "model" in model_artifact:
                self.pipeline = model_artifact["model"]
            else:
                raise ValueError("Artifact de modelo invalido: esperado 'pipeline' ou 'model'")
            self.features = model_artifact.get("features", DEFAULT_FEATURES)
            self.index_weights = model_artifact.get("index_weights", {"baixa": 0.0, "media": 50.0, "alta": 100.0})
            self.classes = model_artifact.get("classes")
            self.static_context = {**zoning_context, **model_artifact.get("static_context", {})}
        else:
            self.pipeline = model_artifact
            self.features = DEFAULT_FEATURES
            self.index_weights = {"baixa": 0.0, "media": 50.0, "alta": 100.0}
            self.classes = None
            self.static_context = zoning_context

        self.publisher = publisher
        self.input_topics = input_topics
        self.expected_topics = set(input_topics)
        self.output_topic = output_topic
        self.service_urn = service_urn
        self.property_kind = property_kind
        self.latest_by_topic: dict[str, dict[str, Any]] = {}

    def _parse_regional_pressure_input(self, message: dict[str, Any]) -> dict[str, Any]:
        result = _extract_result_block(message)
        payload = _extract_payload_block(message)
        inputs_summary = payload.get("inputs_summary", {}) if isinstance(payload.get("inputs_summary"), dict) else {}
        class_probs = (
            payload.get("class_probabilities", {}) if isinstance(payload.get("class_probabilities"), dict) else {}
        )

        timestamp = _extract_timestamp(message)
        hour = None
        weekday = None
        parsed = pd.to_datetime(timestamp, errors="coerce")
        if not pd.isna(parsed):
            hour = int(parsed.hour)
            weekday = int(parsed.dayofweek)

        return {
            "pressure_class": result.get("saref:hasValue"),
            "pressure_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "pressure_index": _safe_float(result.get("urn:ufcity:pressureIndex"))
            if result.get("urn:ufcity:pressureIndex") is not None
            else _safe_float(payload.get("pressure_index")),
            "traffic_level": inputs_summary.get("traffic_level"),
            "bus_supply_level": inputs_summary.get("bus_supply_level"),
            "pressure_prob_baixa": _safe_float(class_probs.get("baixa")),
            "pressure_prob_media": _safe_float(class_probs.get("media")),
            "pressure_prob_alta": _safe_float(class_probs.get("alta")),
            "pressure_hour": _safe_int(hour),
            "pressure_weekday": _safe_int(weekday),
        }

    def _parse_social_accessibility_input(self, message: dict[str, Any]) -> dict[str, Any]:
        result = _extract_result_block(message)
        payload = _extract_payload_block(message)
        inputs_summary = payload.get("inputs_summary", {}) if isinstance(payload.get("inputs_summary"), dict) else {}
        class_probs = (
            payload.get("class_probabilities", {}) if isinstance(payload.get("class_probabilities"), dict) else {}
        )

        timestamp = _extract_timestamp(message)
        hour = None
        weekday = None
        parsed = pd.to_datetime(timestamp, errors="coerce")
        if not pd.isna(parsed):
            hour = int(parsed.hour)
            weekday = int(parsed.dayofweek)

        return {
            "social_class": result.get("saref:hasValue"),
            "social_confidence": _safe_float(result.get("urn:ufcity:confidence")),
            "social_accessibility_index": _safe_float(result.get("urn:ufcity:socialAccessibilityIndex"))
            if result.get("urn:ufcity:socialAccessibilityIndex") is not None
            else _safe_float(payload.get("social_accessibility_index")),
            "mobility_class": inputs_summary.get("mobility_class"),
            "social_pressure_class": inputs_summary.get("pressure_class"),
            "social_traffic_status": inputs_summary.get("traffic_status"),
            "social_bus_status": inputs_summary.get("bus_status"),
            "social_traffic_level": inputs_summary.get("traffic_level"),
            "social_bus_supply_level": inputs_summary.get("bus_supply_level"),
            "social_prob_baixa": _safe_float(class_probs.get("baixa")),
            "social_prob_media": _safe_float(class_probs.get("media")),
            "social_prob_alta": _safe_float(class_probs.get("alta")),
            "social_hour": _safe_int(hour),
            "social_weekday": _safe_int(weekday),
        }

    def _build_model_input(self) -> pd.DataFrame:
        social_topic = self.input_topics[0]
        pressure_topic = self.input_topics[1]
        social_features = self.latest_by_topic[social_topic]["features"]
        pressure_features = self.latest_by_topic[pressure_topic]["features"]

        row = {**pressure_features, **social_features}
        for key, value in self.static_context.items():
            row.setdefault(key, value)

        ordered = {feature: row.get(feature) for feature in self.features}
        return pd.DataFrame([ordered], columns=self.features)

    def _compute_priority_index(self, proba: np.ndarray, classes: list[str]) -> float:
        weights = np.array([float(self.index_weights.get(cls, 50.0)) for cls in classes], dtype=float)
        value = float(np.dot(proba, weights))
        return float(np.clip(value, 0.0, 100.0))

    def _build_output_message(
        self,
        *,
        prediction: str,
        confidence: float | None,
        priority_index: float | None,
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
                "pressure_class": self.latest_by_topic[self.input_topics[1]]["features"].get("pressure_class"),
                "traffic_level": self.latest_by_topic[self.input_topics[1]]["features"].get("traffic_level"),
                "bus_supply_level": self.latest_by_topic[self.input_topics[1]]["features"].get(
                    "bus_supply_level"
                ),
                "social_class": self.latest_by_topic[self.input_topics[0]]["features"].get("social_class"),
                "mobility_class": self.latest_by_topic[self.input_topics[0]]["features"].get("mobility_class"),
                "social_pressure_class": self.latest_by_topic[self.input_topics[0]]["features"].get(
                    "social_pressure_class"
                ),
                "traffic_status": self.latest_by_topic[self.input_topics[0]]["features"].get("social_traffic_status"),
                "bus_status": self.latest_by_topic[self.input_topics[0]]["features"].get("social_bus_status"),
            }
        }
        if priority_index is not None:
            payload["priority_index"] = round(priority_index, 3)
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
        if priority_index is not None:
            message["saref:hasResult"]["urn:ufcity:priorityIndex"] = round(float(priority_index), 3)

        return message

    def update(self, topic: str, message: Any) -> None:
        if not isinstance(message, dict):
            return

        if topic not in self.expected_topics:
            return

        if topic == self.input_topics[0]:
            parsed_features = self._parse_social_accessibility_input(message)
        else:
            parsed_features = self._parse_regional_pressure_input(message)

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
        priority_index = None
        class_probabilities = None
        if hasattr(self.pipeline, "predict_proba"):
            proba = self.pipeline.predict_proba(model_input)[0]
            if self.classes:
                classes = [str(c) for c in self.classes]
            else:
                model = getattr(self.pipeline, "named_steps", {}).get("model")
                classes = [str(c) for c in model.classes_] if model is not None else []
            if not classes:
                classes = ["baixa", "media", "alta"]
            confidence = float(np.max(proba))
            priority_index = self._compute_priority_index(proba=proba, classes=classes)
            class_probabilities = {cls: round(float(p), 6) for cls, p in zip(classes, proba)}

        output_message = self._build_output_message(
            prediction=prediction,
            confidence=confidence,
            priority_index=priority_index,
            class_probabilities=class_probabilities,
        )
        self.publisher.publish_single(self.output_topic, output_message)
        print(
            f"[{SERVICE_NAME}] publicado prioridade={prediction} "
            f"index={None if priority_index is None else round(priority_index, 3)} "
            f"topic={self.output_topic}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo nao encontrado em {MODEL_PATH}")

    sub_topics_env = os.getenv("MQTT_SUB_TOPICS", os.getenv("MQTT_SUB_TOPIC"))
    input_topics = _parse_topics(sub_topics_env, DEFAULT_INPUT_TOPICS)
    if len(input_topics) != 2:
        raise ValueError(
            "Este servico exige exatamente 2 topicos de entrada: social-accessibility-index e regional-mobility-pressure"
        )

    output_topic = os.getenv("MQTT_PUB_TOPIC", DEFAULT_OUTPUT_TOPIC)
    service_urn = os.getenv("SERVICE_URN", DEFAULT_SERVICE_URN)
    property_kind = os.getenv("SEMANTIC_PROPERTY_KIND", DEFAULT_PROPERTY_KIND)
    mqtt_host = os.getenv("MQTT_HOST", "localhost")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    mqtt_client_id = os.getenv("MQTT_CLIENT_ID", SERVICE_NAME)

    model_artifact = joblib.load(MODEL_PATH)
    zoning_context = _load_zoning_factor(ZONING_PATH)

    publisher = MqttPublish({"broker_address": mqtt_host, "port": mqtt_port})
    observer = StrategicInterventionPriorityService(
        model_artifact=model_artifact,
        publisher=publisher,
        input_topics=input_topics,
        output_topic=output_topic,
        service_urn=service_urn,
        property_kind=property_kind,
        zoning_context=zoning_context,
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
