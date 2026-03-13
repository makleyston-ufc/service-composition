from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

SERVICE_FILE = Path(__file__).resolve()
IMPORT_ROOT_CANDIDATES = [
    SERVICE_FILE.parents[2],  # monorepo: services/
    SERVICE_FILE.parents[1],  # container: /app
]
for candidate in IMPORT_ROOT_CANDIDATES:
    if (candidate / "ufcity_microservice_lib").exists():
        if str(candidate) not in sys.path:
            sys.path.insert(0, str(candidate))
        break

from ufcity_microservice_lib import IoTStreamMessageParser, MqttClient, MqttPublish, Observer


MODEL_PATH = Path(__file__).resolve().parent / "model.joblib"
SERVICE_NAME = "bus-regional-supply"
BROKER_HOST = os.getenv("MQTT_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", SERVICE_NAME)
INPUT_TOPIC = os.getenv("MQTT_SUB_TOPIC", "service/mobility/observation/bus-telemetry")
OUTPUT_TOPIC = os.getenv("MQTT_PUB_TOPIC", "service/mobility/inference/bus-regional-supply")
SERVICE_URN = os.getenv("SERVICE_URN", "urn:ufcity:service:bus-regional-supply")
SEMANTIC_PROPERTY_KIND = os.getenv("SEMANTIC_PROPERTY_KIND", "urn:ufcity:propertykind:BusRegionalSupply")


def _extract_raw_payload(message: Any) -> dict[str, Any]:
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


def _extract_timestamp(message: Any, parsed_data: dict[str, Any]) -> str:
    timestamp = parsed_data.get("timestamp")
    if isinstance(timestamp, str) and timestamp:
        return timestamp

    if isinstance(message, dict):
        result_time = message.get("sosa:resultTime")
        if isinstance(result_time, dict):
            value = result_time.get("@value")
            if isinstance(value, str) and value:
                return value
        if isinstance(result_time, str) and result_time:
            return result_time

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _build_output_message(
    prediction: str,
    timestamp: str,
    source_observation_id: str | None,
    confidence: float | None,
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    message: dict[str, Any] = {
        "@context": {
            "iot-stream": "http://purl.org/iot/ontology/iot-stream#",
            "ssn": "http://www.w3.org/ns/ssn/",
            "saref": "https://saref.etsi.org/core/",
        },
        "@id": f"{SERVICE_URN}:obs:{timestamp}",
        "@type": ["iot-stream:StreamObservation", "saref:Observation"],
        "iot-stream:belongsTo": SERVICE_URN,
        "saref:hasPropertyOfInterest": {
            "@type": "saref:PropertyOfInterest",
            "saref:hasPropertyKind": SEMANTIC_PROPERTY_KIND,
        },
        "saref:hasResult": {
            "@type": "saref:PropertyValue",
            "saref:hasValue": prediction,
        },
        "saref:hasTimestamp": timestamp,
        "urn:ufcity:payload": input_payload,
    }

    if source_observation_id:
        message["iot-stream:derivedFrom"] = {
            "@type": "iot-stream:Analytics",
            "ssn:hasInput": [source_observation_id],
        }

    if confidence is not None:
        message["saref:hasResult"]["urn:ufcity:confidence"] = round(float(confidence), 6)

    return message


class BusRegionalSupplyService(Observer):
    def __init__(self, model_artifact: Any, publisher: MqttPublish):
        self.model = model_artifact["pipeline"] if isinstance(model_artifact, dict) and "pipeline" in model_artifact else model_artifact
        self.publisher = publisher
        self.topic_key = INPUT_TOPIC.replace("/", "__")

    def update(self, topic: str, message: Any) -> None:
        parsed_data = IoTStreamMessageParser.extract_bus_features(message)
        raw_payload = _extract_raw_payload(message)
        if not parsed_data or not raw_payload:
            print("[bus-regional-supply] mensagem ignorada: payload sem formato esperado")
            return

        model_input = [{self.topic_key: raw_payload}]
        prediction = self.model.predict(model_input)[0]

        confidence = None
        if hasattr(self.model, "predict_proba"):
            proba = self.model.predict_proba(model_input)[0]
            confidence = float(max(proba))

        timestamp = _extract_timestamp(message, parsed_data)
        source_observation_id = _extract_observation_id(message)
        output_message = _build_output_message(
            prediction=str(prediction),
            timestamp=timestamp,
            source_observation_id=source_observation_id,
            confidence=confidence,
            input_payload=raw_payload,
        )
        self.publisher.publish_single(OUTPUT_TOPIC, output_message)
        print(
            "[bus-regional-supply] publicado supply="
            f"{prediction} output_topic={OUTPUT_TOPIC}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo não encontrado em {MODEL_PATH}")

    model_artifact = joblib.load(MODEL_PATH)
    publisher = MqttPublish({"broker_address": BROKER_HOST, "port": BROKER_PORT})
    service_observer = BusRegionalSupplyService(model_artifact, publisher)

    client = MqttClient(
        {
            "broker_address": BROKER_HOST,
            "port": BROKER_PORT,
            "client_id": MQTT_CLIENT_ID,
            "topics": [INPUT_TOPIC],
            "qos": 0,
        }
    )
    client.attach(service_observer)
    print(
        "[bus-regional-supply] iniciando serviço com "
        f"sub={INPUT_TOPIC} pub={OUTPUT_TOPIC} broker={BROKER_HOST}:{BROKER_PORT}"
    )
    client.subscribe_to_topics()


if __name__ == "__main__":
    main()
