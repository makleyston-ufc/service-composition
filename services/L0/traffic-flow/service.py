from __future__ import annotations

import os
import re
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
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

from ufcity_microservice_lib import MqttClient, MqttPublish, Observer


MODEL_PATH = Path(__file__).resolve().parent / "traffic_flow_model.joblib"

BROKER_HOST = os.getenv("MQTT_HOST", "localhost")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "traffic-flow")
INPUT_TOPIC = os.getenv("MQTT_SUB_TOPIC", "service/mobility/observation/radar-telemetry")
OUTPUT_TOPIC = os.getenv("MQTT_PUB_TOPIC", "service/mobility/inference/traffic-flow")
SERVICE_URN = os.getenv("SERVICE_URN", "urn:ufcity:service:traffic-flow")
SEMANTIC_PROPERTY_KIND = os.getenv("SEMANTIC_PROPERTY_KIND", "urn:ufcity:propertykind:TrafficFlowLevel")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "15"))

DEFAULT_FEATURES = [
    "ID EQP",
    "SENTIDO",
    "FAIXA",
    "vehicle_count",
    "avg_speed",
    "speed_std",
    "pct_moto",
    "pct_heavy",
    "hour",
    "weekday",
]

HEAVY_VEHICLE_PATTERN = re.compile(r"CAMINH[AÃ]O|ÔNIBUS|ONIBUS", re.IGNORECASE)


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


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        pass

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _to_iso_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").lower() or "unknown"


def _extract_result_block(message: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "sosa:hasSimpleResult",
        "iotstream:hasSimpleResult",
        "iot-stream:hasSimpleResult",
        "hasSimpleResult",
    )
    for key in keys:
        value = message.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_result_time(message: dict[str, Any]) -> str | None:
    result_time = message.get("sosa:resultTime") or message.get("resultTime")
    if isinstance(result_time, dict):
        value = result_time.get("@value")
        if isinstance(value, str) and value:
            return value
    if isinstance(result_time, str) and result_time:
        return result_time
    return None


class TrafficFlowService(Observer):
    def __init__(self, model_artifact: dict[str, Any], publisher: MqttPublish):
        self.pipeline = model_artifact.get("pipeline")
        self.features = model_artifact.get("features") or DEFAULT_FEATURES
        self.publisher = publisher
        self.window = timedelta(minutes=WINDOW_MINUTES)
        self.events_by_key: dict[tuple[int, str, int], deque[dict[str, Any]]] = defaultdict(deque)

    def _extract_input(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict):
            return None

        result = _extract_result_block(message)
        row = result if result else message

        id_eqp = _to_int(row.get("idEquipamento") or row.get("ID EQP"))
        faixa = _to_int(row.get("faixa") or row.get("FAIXA"))
        sentido_raw = row.get("sentido") or row.get("SENTIDO")
        sentido = str(sentido_raw).strip() if sentido_raw is not None else ""
        if not sentido:
            sentido = "unknown"

        if id_eqp is None or faixa is None:
            return None

        data_hora = row.get("dataHora") or row.get("DATA HORA")
        milisegundo = _to_int(row.get("milisegundo") or row.get("MILESEGUNDO") or 0) or 0
        event_dt = _parse_datetime(data_hora)
        if event_dt is not None:
            event_dt = event_dt + timedelta(milliseconds=max(0, min(999, milisegundo)))

        result_time = _extract_result_time(message)
        if event_dt is None and result_time:
            event_dt = _parse_datetime(result_time)
        if event_dt is None:
            event_dt = datetime.now(timezone.utc)

        timestamp_iso = result_time or _to_iso_utc(event_dt)

        return {
            "id_eqp": id_eqp,
            "faixa": faixa,
            "sentido": sentido,
            "velocidade": _to_float(row.get("velocidadeAferida") or row.get("VELOCIDADE AFERIDA")),
            "classificacao": str(row.get("classificacao") or row.get("CLASSIFICAÇÃO") or ""),
            "event_dt": event_dt,
            "timestamp_iso": timestamp_iso,
            "source_observation_id": message.get("@id"),
        }

    def _append_and_compact_window(self, key: tuple[int, str, int], event: dict[str, Any]) -> deque[dict[str, Any]]:
        events = self.events_by_key[key]
        events.append(event)

        cutoff = event["event_dt"] - self.window
        while events and events[0]["event_dt"] < cutoff:
            events.popleft()

        return events

    def _build_feature_row(
        self,
        key: tuple[int, str, int],
        events: deque[dict[str, Any]],
        event_dt: datetime,
    ) -> dict[str, Any]:
        id_eqp, sentido, faixa = key
        vehicle_count = len(events)

        speeds = [item["velocidade"] for item in events if item.get("velocidade") is not None]
        avg_speed = sum(speeds) / len(speeds) if speeds else None
        if len(speeds) >= 2:
            mean_speed = avg_speed if avg_speed is not None else 0.0
            variance = sum((speed - mean_speed) ** 2 for speed in speeds) / (len(speeds) - 1)
            speed_std = variance**0.5
        else:
            speed_std = None

        moto_count = sum(1 for item in events if str(item.get("classificacao", "")).upper() == "MOTO")
        heavy_count = sum(
            1 for item in events if HEAVY_VEHICLE_PATTERN.search(str(item.get("classificacao", "")))
        )
        pct_moto = (moto_count / vehicle_count) if vehicle_count else 0.0
        pct_heavy = (heavy_count / vehicle_count) if vehicle_count else 0.0

        return {
            "ID EQP": id_eqp,
            "SENTIDO": sentido,
            "FAIXA": faixa,
            "vehicle_count": vehicle_count,
            "avg_speed": avg_speed,
            "speed_std": speed_std,
            "pct_moto": pct_moto,
            "pct_heavy": pct_heavy,
            "hour": int(event_dt.hour),
            "weekday": int(event_dt.weekday()),
        }

    def _build_output_message(
        self,
        prediction: str,
        confidence: float | None,
        source_observation_id: str | None,
        timestamp_iso: str,
        row: dict[str, Any],
    ) -> dict[str, Any]:
        segment_id = f"{row['ID EQP']}:{_slugify(str(row['SENTIDO']))}:{row['FAIXA']}"
        obs_id = f"{SERVICE_URN}:obs:{segment_id}:{timestamp_iso}"

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
            "saref:hasFeatureOfInterest": f"urn:ufcity:traffic:segment:{segment_id}",
            "saref:hasPropertyOfInterest": {
                "@type": "saref:PropertyOfInterest",
                "saref:hasPropertyKind": SEMANTIC_PROPERTY_KIND,
            },
            "saref:hasResult": {
                "@type": "saref:PropertyValue",
                "saref:hasValue": prediction,
            },
            "saref:hasTimestamp": timestamp_iso,
            "sosa:resultTime": {
                "@type": "xsd:dateTime",
                "@value": timestamp_iso,
            },
            "urn:ufcity:payload": {
                "windowMinutes": WINDOW_MINUTES,
                "features": row,
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
        parsed = self._extract_input(message)
        if not parsed:
            print("[traffic-flow] mensagem ignorada: payload sem formato esperado")
            return

        key = (parsed["id_eqp"], parsed["sentido"], parsed["faixa"])
        events = self._append_and_compact_window(key, parsed)
        row = self._build_feature_row(key, events, parsed["event_dt"])
        model_input = pd.DataFrame([row], columns=self.features)

        prediction = str(self.pipeline.predict(model_input)[0])
        confidence = None
        if hasattr(self.pipeline, "predict_proba"):
            probabilities = self.pipeline.predict_proba(model_input)[0]
            confidence = float(max(probabilities))

        output_message = self._build_output_message(
            prediction=prediction,
            confidence=confidence,
            source_observation_id=parsed["source_observation_id"],
            timestamp_iso=parsed["timestamp_iso"],
            row=row,
        )

        self.publisher.publish_single(OUTPUT_TOPIC, output_message)
        print(
            "[traffic-flow] publicado nivel="
            f"{prediction} key={key} topic={OUTPUT_TOPIC}"
        )


def main() -> None:
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Modelo nao encontrado em {MODEL_PATH}")

    model_artifact = joblib.load(MODEL_PATH)
    publisher = MqttPublish({"broker_address": BROKER_HOST, "port": BROKER_PORT})
    service_observer = TrafficFlowService(model_artifact, publisher)

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
        "[traffic-flow] iniciando servico com "
        f"sub={INPUT_TOPIC} pub={OUTPUT_TOPIC} broker={BROKER_HOST}:{BROKER_PORT}"
    )
    client.subscribe_to_topics()


if __name__ == "__main__":
    main()
