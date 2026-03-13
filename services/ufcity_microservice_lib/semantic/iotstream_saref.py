from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


class IoTStreamMessageParser:
    @staticmethod
    def _get_result_block(message: dict[str, Any]) -> dict[str, Any]:
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

    @staticmethod
    def _extract_timestamp(message: dict[str, Any], row: dict[str, Any]) -> str:
        result_time = message.get("sosa:resultTime") or message.get("resultTime")
        if isinstance(result_time, dict):
            value = result_time.get("@value")
            if isinstance(value, str) and value:
                return value
        if isinstance(result_time, str) and result_time:
            return result_time

        hr_raw = row.get("HR")
        hr = str(hr_raw) if hr_raw is not None else ""
        if len(hr) == 14 and hr.isdigit():
            dt = datetime.strptime(hr, "%Y%m%d%H%M%S")
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    @classmethod
    def extract_bus_features(cls, message: Any) -> dict[str, Any]:
        if not isinstance(message, dict):
            return {}

        result = cls._get_result_block(message)
        row = result if result else message

        timestamp = cls._extract_timestamp(message, row)
        source_observation_id = message.get("@id")

        return {
            "EV": row.get("EV"),
            "HR": row.get("HR"),
            "LT": row.get("LT"),
            "LG": row.get("LG"),
            "NV": row.get("NV"),
            "VL": row.get("VL"),
            "NL": row.get("NL"),
            "DG": row.get("DG"),
            "SV": row.get("SV"),
            "DT": row.get("DT"),
            "timestamp": timestamp,
            "source_observation_id": source_observation_id,
            "stream_id": message.get("iotstream:stream") or message.get("iot-stream:stream"),
        }


class IoTStreamSarefAnnotator:
    def __init__(self, service_urn: str):
        self.service_urn = service_urn

    def build_operation_status_observation(
        self,
        vehicle_id: str,
        status_value: str,
        timestamp_iso: str,
        source_observation_id: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        obs_id = f"{self.service_urn}:obs:{vehicle_id}:{timestamp_iso}"

        payload: dict[str, Any] = {
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
            "iot-stream:belongsTo": self.service_urn,
            "saref:hasFeatureOfInterest": f"urn:ufcity:bus:vehicle:{vehicle_id}",
            "saref:hasPropertyOfInterest": {
                "@type": "saref:PropertyOfInterest",
                "saref:hasPropertyKind": "urn:ufcity:propertykind:BusOperationStatus",
            },
            "saref:hasResult": {
                "@type": "saref:PropertyValue",
                "saref:hasValue": status_value,
            },
            "saref:hasTimestamp": timestamp_iso,
            "sosa:resultTime": {
                "@type": "xsd:dateTime",
                "@value": timestamp_iso,
            },
        }

        if source_observation_id:
            payload["iot-stream:derivedFrom"] = {
                "@type": "iot-stream:Analytics",
                "ssn:hasInput": [source_observation_id],
            }

        if confidence is not None:
            payload["saref:hasResult"]["urn:ufcity:confidence"] = round(float(confidence), 6)

        return payload
