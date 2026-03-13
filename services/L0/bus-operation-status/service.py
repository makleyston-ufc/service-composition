from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd

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

from ufcity_microservice_lib import (
    IoTStreamMessageParser,
    IoTStreamSarefAnnotator,
    MqttClient,
    MqttPublish,
    Observer,
)


MODEL_PATH = Path(__file__).resolve().parent / "bus_status_model.joblib"

BROKER_HOST = os.getenv("MQTT_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "bus-operation-status")
INPUT_TOPIC = os.getenv("MQTT_SUB_TOPIC", "service/mobility/observation/bus-telemetry")
OUTPUT_TOPIC = os.getenv("MQTT_PUB_TOPIC", "service/mobility/inference/bus-operation-status")
SERVICE_URN = os.getenv("SERVICE_URN", "urn:ufcity:service:bus-operation-status")


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
        return int(value)
    except (TypeError, ValueError):
        return None


def _hr_to_hour_weekday(hr: Any) -> tuple[int | None, int | None]:
    hr_str = str(hr) if hr is not None else ""
    if len(hr_str) == 14 and hr_str.isdigit():
        dt = datetime.strptime(hr_str, "%Y%m%d%H%M%S")
        return dt.hour, dt.weekday()
    return None, None


class BusOperationStatusService(Observer):
    def __init__(self, model_artifact: dict[str, Any], publisher: MqttPublish):
        self.pipeline = model_artifact["pipeline"]
        self.features = model_artifact["features"]
        self.publisher = publisher
        self.annotator = IoTStreamSarefAnnotator(service_urn=SERVICE_URN)

    def _build_model_input(self, data: dict[str, Any]) -> pd.DataFrame:
        hour, weekday = _hr_to_hour_weekday(data.get("HR"))

        row = {
            "LT": _to_float(data.get("LT")),
            "LG": _to_float(data.get("LG")),
            "DG": _to_float(data.get("DG")),
            "SV": data.get("SV"),
            "DT": _to_float(data.get("DT")),
            "hour": _to_int(hour),
            "weekday": _to_int(weekday),
            "NL": data.get("NL"),
        }
        return pd.DataFrame([row], columns=self.features)

    def update(self, topic: str, message: Any) -> None:
        data = IoTStreamMessageParser.extract_bus_features(message)
        if not data:
            print("[bus-operation-status] mensagem ignorada: payload sem formato esperado")
            return

        model_input = self._build_model_input(data)
        prediction = self.pipeline.predict(model_input)[0]

        confidence = None
        if hasattr(self.pipeline, "predict_proba"):
            proba = self.pipeline.predict_proba(model_input)[0]
            confidence = float(proba.max())

        timestamp = data.get("timestamp") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        vehicle_id = str(data.get("NV") or "unknown")

        output_message = self.annotator.build_operation_status_observation(
            vehicle_id=vehicle_id,
            status_value=str(prediction),
            timestamp_iso=str(timestamp),
            source_observation_id=data.get("source_observation_id"),
            confidence=confidence,
        )

        self.publisher.publish_single(OUTPUT_TOPIC, output_message)
        print(
            "[bus-operation-status] publicado status="
            f"{prediction} vehicle={vehicle_id} output_topic={OUTPUT_TOPIC}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo não encontrado em {MODEL_PATH}")

    model_artifact = joblib.load(MODEL_PATH)
    publisher = MqttPublish({"broker_address": BROKER_HOST, "port": BROKER_PORT})
    service_observer = BusOperationStatusService(model_artifact, publisher)

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
        "[bus-operation-status] iniciando serviço com "
        f"sub={INPUT_TOPIC} pub={OUTPUT_TOPIC} broker={BROKER_HOST}:{BROKER_PORT}"
    )
    client.subscribe_to_topics()


if __name__ == "__main__":
    main()
