# Strategic Intervention Priority

## Description

L3 service that infers **strategic intervention priority** from the L1/L2 signals below:

- regional mobility pressure (`regional-mobility-pressure`)
- social accessibility index (`social-accessibility-index`)

The service consumes the MQTT input topics, builds a feature vector, runs a `scikit-learn` pipeline from `model.joblib`, and publishes the final classification.

## Domain

`planning`

## Semantic vocabularies

`IoT-Stream`, `SOSA/SSN`, `SAREF`

## MQTT topics

### Inputs

- `service/mobility/inference/regional-mobility-pressure`
- `service/social/inference/social-accessibility-index`

### Output

- `service/planning/inference/strategic-intervention-priority`

## ML model

- Artifact: `/home/makleyston/Projects/service-composition/services/L3/strategic-intervention-priority/model.joblib`
- Current type: `sklearn.pipeline.Pipeline`
- Predicted classes: `baixa`, `media`, `alta`

In addition to the class, the service also computes:

- `urn:ufcity:confidence`: highest class probability
- `urn:ufcity:priorityIndex`: continuous index (0-100) derived from class probabilities

## Training data sources (not service inputs)

The model was built using the public zoning dataset below together with outputs from the L1/L2 services listed above. These sources are not consumed as runtime inputs by this service.

```
https://dados.pbh.gov.br/dataset/zoneamento-lei-11181
```

## How the service works (inference cycle)

1. When a message arrives on any input topic, the service parses it and stores the latest features.
2. Once both topics have data, a new inference is triggered.
3. The result is published to `service/planning/inference/strategic-intervention-priority`.

## Current rules and limitations

- No staleness timeout (old data is not automatically invalidated).
- No aggregation/event-time window.
- No explicit deduplication by `@id`.
- No forced temporal ordering for out-of-order messages.
- Output `timestamp` mirrors the newest input message timestamp.
- No strict time filtering or synchronization policy is implemented.

## Main environment variables

- `MQTT_HOST` (default: `localhost`)
- `MQTT_PORT` (default: `1883`)
- `MQTT_CLIENT_ID` (default: `strategic-intervention-priority`)
- `MQTT_SUB_TOPICS` (default: input topics above)
- `MQTT_PUB_TOPIC` (default: output topic above)
- `SERVICE_URN` (default: `urn:ufcity:service:strategic-intervention-priority`)
- `SEMANTIC_PROPERTY_KIND` (default: `urn:ufcity:propertykind:StrategicInterventionPriority`)
- `MODEL_PATH` (default: `/home/makleyston/Projects/service-composition/services/L3/strategic-intervention-priority/model.joblib`)
- `ZONING_PATH` (default: `/home/makleyston/Projects/service-composition/services/L3/strategic-intervention-priority/dataset/20260101_zoneamento_11181.csv`)

## Output example (summary)

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:strategic-intervention-priority",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:confidence": 0.82,
    "urn:ufcity:priorityIndex": 58.3
  },
  "urn:ufcity:payload": {
    "inputs_summary": {
      "pressure_class": "media",
      "traffic_level": "moderado",
      "bus_supply_level": "alta",
      "social_class": "baixa",
      "mobility_class": "baixa",
      "social_pressure_class": "media",
      "traffic_status": null,
      "bus_status": null
    },
    "class_probabilities": {
      "baixa": 0.10,
      "media": 0.82,
      "alta": 0.08
    }
  }
}
```
