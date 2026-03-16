# Social Accessibility Index

## Description

L2 service that infers **social accessibility** from two L1 signals:

- mobility efficiency index (`mobility-efficiency-index`)
- regional mobility pressure (`regional-mobility-pressure`)

The service consumes the MQTT input topics, builds a feature vector, runs a `scikit-learn` pipeline from `model.joblib`, and publishes the final classification.

## Domain

`social`

## Semantic vocabularies

`IoT-Stream`, `SOSA/SSN`, `SAREF`

## MQTT topics

### Inputs

- `service/mobility/inference/mobility-efficiency-index`
- `service/mobility/inference/regional-mobility-pressure`

### Output

- `service/social/inference/social-accessibility-index`

## ML model

- Artifact: `/home/makleyston/Projects/service-composition/services/L2/social-accessibility-index/model.joblib`
- Current type: `sklearn.pipeline.Pipeline`
- Predicted classes: `baixa`, `media`, `alta`

In addition to the class, the service also computes:

- `urn:ufcity:confidence`: highest class probability
- `urn:ufcity:socialAccessibilityIndex`: continuous index (0-100) derived from class probabilities

## Training data sources (not service inputs)

The model was built using the public dataset below together with outputs from the L1 `mobility-efficiency-index` and `regional-mobility-pressure` services. These sources are not consumed as runtime inputs by this service.

```
https://dados.pbh.gov.br/dataset/territorio-cras
```

## How the service works (inference cycle)

1. When a message arrives on the input topics, the service parses both streams and builds the feature vector.
2. Each new complete pair of messages triggers a new inference.
3. The result is published to `service/social/inference/social-accessibility-index`.

## Current rules and limitations

- No staleness timeout (old data is not automatically invalidated).
- No aggregation/event-time window.
- No explicit deduplication by `@id`.
- No forced temporal ordering for out-of-order messages.
- Output `timestamp` mirrors the latest input message timestamp.
- No strict time filtering or synchronization policy is implemented.

## Main environment variables

- `MQTT_HOST` (default: `localhost`)
- `MQTT_PORT` (default: `1883`)
- `MQTT_CLIENT_ID` (default: `social-accessibility-index`)
- `MQTT_SUB_TOPICS` (default: input topics above)
- `MQTT_PUB_TOPIC` (default: output topic above)
- `SERVICE_URN` (default: `urn:ufcity:service:social-accessibility-index`)
- `SEMANTIC_PROPERTY_KIND` (default: `urn:ufcity:propertykind:SocialAccessibilityIndex`)

## Output example (summary)

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:social-accessibility-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:confidence": 0.79,
    "urn:ufcity:socialAccessibilityIndex": 57.2
  },
  "urn:ufcity:payload": {
    "inputs_summary": {
      "mobility_class": "media",
      "traffic_status": "livre",
      "bus_status": "parado",
      "pressure_class": "baixa",
      "traffic_level": "baixo",
      "bus_supply_level": "alta"
    },
    "class_probabilities": {
      "baixa": 0.12,
      "media": 0.79,
      "alta": 0.09
    }
  }
}
```
