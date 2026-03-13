# bus-regional-supply

## Description

Infers regional bus supply level from bus telemetry observations.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/observation/bus-telemetry`

## Output Topic

`service/mobility/inference/bus-regional-supply`

## ML Model

Scikit-learn Pipeline loaded from `model.joblib` (current trained estimator: `LogisticRegression`).

## Possible Outputs

- `baixa`
- `media`
- `alta`

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:bus-regional-supply",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media"
  }
}
```
