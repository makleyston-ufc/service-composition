from __future__ import annotations

import os
import sys
from pathlib import Path

SERVICE_FILE = Path(__file__).resolve()
IMPORT_ROOT_CANDIDATES = [SERVICE_FILE.parents[2], SERVICE_FILE.parents[1]]
for candidate in IMPORT_ROOT_CANDIDATES:
    if (candidate / "ufcity_microservice_lib").exists() and str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))
        break

from ufcity_microservice_lib import ServiceConfig, run_generic_service

SERVICE_NAME = "mobility-efficiency-index"
DEFAULT_INPUT_TOPICS = [
    "service/mobility/inference/traffic-speed",
    "service/mobility/inference/bus-operation-status",
]
DEFAULT_OUTPUT_TOPIC = "service/mobility/inference/mobility-efficiency-index"


def _parse_topics(raw_topics: str | None, defaults: list[str]) -> list[str]:
    if not raw_topics:
        return defaults
    parsed = [topic.strip() for topic in raw_topics.split(",") if topic.strip()]
    return parsed or defaults


if __name__ == "__main__":
    sub_topics_env = os.getenv("MQTT_SUB_TOPICS", os.getenv("MQTT_SUB_TOPIC"))
    config = ServiceConfig(
        service_name=SERVICE_NAME,
        service_urn=os.getenv("SERVICE_URN", "urn:ufcity:service:mobility-efficiency-index"),
        input_topics=_parse_topics(sub_topics_env, DEFAULT_INPUT_TOPICS),
        output_topic=os.getenv("MQTT_PUB_TOPIC", DEFAULT_OUTPUT_TOPIC),
        semantic_property_kind=os.getenv("SEMANTIC_PROPERTY_KIND", "urn:ufcity:propertykind:MobilityEfficiencyIndex"),
        model_path=SERVICE_FILE.parent / "model.joblib",
        mqtt_host=os.getenv("MQTT_HOST", "mosquitto"),
        mqtt_port=int(os.getenv("MQTT_PORT", "1883")),
        mqtt_client_id=os.getenv("MQTT_CLIENT_ID", SERVICE_NAME),
    )
    run_generic_service(config)
