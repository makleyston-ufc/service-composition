# regional-mobility-pressure

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

`service/mobility/inference/regional-mobility-pressure`

## ML Model

Scikit-learn Pipeline loaded from `model.joblib` (current trained estimator: `RandomForestClassifier`).

## Possible Outputs

- `baixa`
- `media`
- `alta`

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:regional-mobility-pressure",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:pressureIndex": 65.8
  }
}
```
