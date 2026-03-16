# Rrban Risk Index

## Description

L2 service that infers **urban infrastructure risk** from an L1 signal:

- urban congestion index (`urban-congestion-index`)

The service consumes the MQTT input topic, builds a feature vector, runs a `scikit-learn` pipeline from `model.joblib`, and publishes the final classification.

## Domain

`infrastructure`

## Semantic vocabularies

`IoT-Stream`, `SOSA/SSN`, `SAREF`

## MQTT topics

### Inputs

- `service/mobility/inference/urban-congestion-index`

### Output

- `service/infrastructure/inference/urban-risk-index`

## ML model

- Artifact: `/home/makleyston/Projects/service-composition/services/L1/mobility-efficiency-index/model.joblib`
- Current type: `sklearn.pipeline.Pipeline`
- Predicted classes: `baixa`, `media`, `alta`

In addition to the class, the service also computes:

- `urn:ufcity:confidence`: highest class probability
- `urn:ufcity:riskIndex`: continuous index (0-100) derived from class probabilities

## Training data sources (not service inputs)

The model was built using the public lighting dataset below together with outputs from the L1 `urban-congestion-index` service. These sources are not consumed as runtime inputs by this service.

```
https://dados.pbh.gov.br/dataset/unidade-de-iluminacao-publica
```

## How the service works (inference cycle)

1. When a message arrives on the input topic, the service parses it and builds the feature vector.
2. Each new message triggers a new inference.
3. The result is published to `service/infrastructure/inference/urban-risk-index`.

## Current rules and limitations

- No staleness timeout (old data is not automatically invalidated).
- No aggregation/event-time window.
- No explicit deduplication by `@id`.
- No forced temporal ordering for out-of-order messages.
- Output `timestamp` mirrors the input message timestamp.
- No strict time filtering or synchronization policy is implemented.

## Main environment variables

- `MQTT_HOST` (default: `localhost`)
- `MQTT_PORT` (default: `1883`)
- `MQTT_CLIENT_ID` (default: `urban-risk-index`)
- `MQTT_SUB_TOPICS` (default: input topic above)
- `MQTT_PUB_TOPIC` (default: output topic above)
- `SERVICE_URN` (default: `urn:ufcity:service:urban-risk-index`)
- `SEMANTIC_PROPERTY_KIND` (default: `urn:ufcity:propertykind:UrbanRiskIndex`)

## Output example (summary)

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:urban-risk-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:confidence": 0.82,
    "urn:ufcity:riskIndex": 58.3
  },
  "urn:ufcity:payload": {
    "inputs_summary": {
      "congestion_class": "moderado",
      "flow_level": "moderado",
      "speed_level": "livre"
    },
    "class_probabilities": {
      "baixa": 0.10,
      "media": 0.82,
      "alta": 0.08
    }
  }
}
```
