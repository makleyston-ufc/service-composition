from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

SERVICE_FILE = Path(__file__).resolve()
IMPORT_ROOT_CANDIDATES = [SERVICE_FILE.parents[2], SERVICE_FILE.parents[1]]
for candidate in IMPORT_ROOT_CANDIDATES:
    if (candidate / "ufcity_microservice_lib").exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))
        break

from ufcity_microservice_lib import MqttClient, MqttPublish, Observer

MODEL_PATH = SERVICE_FILE.parent / "model.joblib"

BROKER_HOST = os.getenv("MQTT_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "traffic-speed")
OUTPUT_TOPIC = os.getenv("MQTT_PUB_TOPIC", "service/mobility/inference/traffic-speed")
SERVICE_URN = os.getenv("SERVICE_URN", "urn:ufcity:service:traffic-speed")
PROPERTY_KIND = os.getenv("SEMANTIC_PROPERTY_KIND", "urn:ufcity:propertykind:TrafficSpeedStatus")


def _parse_topics(raw_topics: str | None, default_topics: list[str]) -> list[str]:
    if not raw_topics:
        return default_topics
    topics = [topic.strip() for topic in raw_topics.split(",") if topic.strip()]
    return topics or default_topics


def _extract_result_payload(message: Any) -> dict[str, Any]:
    if not isinstance(message, dict):
        return {}

    for key in ("sosa:hasSimpleResult", "iotstream:hasSimpleResult", "iot-stream:hasSimpleResult", "hasSimpleResult"):
        value = message.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_timestamp(message: dict[str, Any], result_payload: dict[str, Any]) -> str:
    result_time = message.get("sosa:resultTime") or message.get("resultTime")
    if isinstance(result_time, dict):
        value = result_time.get("@value")
        if isinstance(value, str) and value:
            return value
    if isinstance(result_time, str) and result_time:
        return result_time

    dt_raw = result_payload.get("dataHora")
    if isinstance(dt_raw, str) and dt_raw:
        try:
            dt = datetime.fromisoformat(dt_raw)
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            pass

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _extract_hour_weekday(data_hora: Any) -> tuple[int | None, int | None]:
    if not isinstance(data_hora, str) or not data_hora:
        return None, None
    try:
        dt = datetime.fromisoformat(data_hora)
    except ValueError:
        return None, None
    return dt.hour, dt.weekday()


class TrafficSpeedService(Observer):
    def __init__(self, model_artifact: dict[str, Any], publisher: MqttPublish):
        self.pipeline = model_artifact["pipeline"]
        self.features = model_artifact["features"]
        self.publisher = publisher

    def _build_model_input(self, payload: dict[str, Any]) -> pd.DataFrame:
        hour, weekday = _extract_hour_weekday(payload.get("dataHora"))

        row = {
            "id_eqp": _to_int(payload.get("idEquipamento")),
            "faixa": _to_int(payload.get("faixa")),
            "id_endereco": _to_int(payload.get("idEndereco")),
            "velocidade_via": _to_float(payload.get("velocidadeVia")),
            "classificacao": payload.get("classificacao"),
            "tamanho": payload.get("tamanho"),
            "sentido": payload.get("sentido"),
            "latitude": _to_float(payload.get("latitude")),
            "longitude": _to_float(payload.get("longitude")),
            "hour": _to_int(hour),
            "weekday": _to_int(weekday),
        }
        return pd.DataFrame([row], columns=self.features)

    def _build_output_message(
        self,
        prediction: str,
        timestamp: str,
        source_observation_id: str | None,
        road_segment_id: str,
        confidence: float | None,
    ) -> dict[str, Any]:
        obs_id = f"{SERVICE_URN}:obs:{road_segment_id}:{timestamp}"
        output: dict[str, Any] = {
            "@context": {
                "iot-stream": "http://purl.org/iot/ontology/iot-stream#",
                "ssn": "http://www.w3.org/ns/ssn/",
                "saref": "https://saref.etsi.org/core/",
                "sosa": "http://www.w3.org/ns/sosa/",
                "xsd": "http://www.w3.org/2001/XMLSchema#",
            },
            "@id": obs_id,
            "@type": [
                "iot-stream:StreamObservation",
                "saref:Observation",
            ],
            "iot-stream:belongsTo": SERVICE_URN,
            "saref:hasFeatureOfInterest": f"urn:ufcity:traffic:road-segment:{road_segment_id}",
            "saref:hasPropertyOfInterest": {
                "@type": "saref:PropertyOfInterest",
                "saref:hasPropertyKind": PROPERTY_KIND,
            },
            "saref:hasResult": {
                "@type": "saref:PropertyValue",
                "saref:hasValue": str(prediction),
            },
            "saref:hasTimestamp": timestamp,
            "sosa:resultTime": {
                "@type": "xsd:dateTime",
                "@value": timestamp,
            },
        }
        if source_observation_id:
            output["iot-stream:derivedFrom"] = {
                "@type": "iot-stream:Analytics",
                "ssn:hasInput": [source_observation_id],
            }
        if confidence is not None:
            output["saref:hasResult"]["urn:ufcity:confidence"] = round(float(confidence), 6)
        return output

    def update(self, topic: str, message: Any) -> None:
        if not isinstance(message, dict):
            print("[traffic-speed] mensagem ignorada: payload nao e dict")
            return

        payload = _extract_result_payload(message)
        if not payload:
            print("[traffic-speed] mensagem ignorada: sem sosa:hasSimpleResult")
            return

        model_input = self._build_model_input(payload)
        prediction = self.pipeline.predict(model_input)[0]

        confidence = None
        if hasattr(self.pipeline, "predict_proba"):
            proba = self.pipeline.predict_proba(model_input)[0]
            confidence = float(proba.max())

        source_observation_id = message.get("@id")
        timestamp = _extract_timestamp(message, payload)
        road_segment_id = str(payload.get("idEndereco") or "unknown")

        output_message = self._build_output_message(
            prediction=str(prediction),
            timestamp=timestamp,
            source_observation_id=str(source_observation_id) if source_observation_id else None,
            road_segment_id=road_segment_id,
            confidence=confidence,
        )

        self.publisher.publish_single(OUTPUT_TOPIC, output_message)
        print(
            "[traffic-speed] publicado traffic_status="
            f"{prediction} road_segment={road_segment_id} output_topic={OUTPUT_TOPIC}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo nao encontrado em {MODEL_PATH}")

    sub_topics_env = os.getenv("MQTT_SUB_TOPICS", os.getenv("MQTT_SUB_TOPIC"))
    input_topics = _parse_topics(sub_topics_env, ["transport/radar/telemetry"])

    model_artifact = joblib.load(MODEL_PATH)
    publisher = MqttPublish({"broker_address": BROKER_HOST, "port": BROKER_PORT})
    service_observer = TrafficSpeedService(model_artifact, publisher)

    client = MqttClient(
        {
            "broker_address": BROKER_HOST,
            "port": BROKER_PORT,
            "client_id": MQTT_CLIENT_ID,
            "topics": input_topics,
            "qos": 0,
        }
    )
    client.attach(service_observer)

    print(
        "[traffic-speed] iniciando servico com "
        f"sub={input_topics} pub={OUTPUT_TOPIC} broker={BROKER_HOST}:{BROKER_PORT}"
    )
    client.subscribe_to_topics()


if __name__ == "__main__":
    main()
