# alternative-regional-pressure

## Description

Infers regional mobility pressure by combining traffic flow and bus regional supply observations.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/traffic-flow`
- `service/mobility/inference/bus-regional-supply`

## Output Topic

- `service/mobility/inference/alternative-regional-pressure`

## ML Model

Scikit-learn Pipeline loaded from `model.joblib` (current trained estimator: `ExtraTreesClassifier`).

## Possible Outputs

- `baixa`
- `media`
- `alta`

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:alternative-regional-pressure",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:pressureIndex": 65.8
  }
}
```
