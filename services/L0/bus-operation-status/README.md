# bus-operation-status

## Description

Infers the operational status of buses from bus telemetry observations.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/observation/bus-telemetry`

## Output Topic

`service/mobility/inference/bus-operation-status`

## ML Model

Scikit-learn Pipeline loaded from `bus_status_model.joblib` (current trained estimator: `RandomForestClassifier`).

## Possible Outputs

- `lento`
- `normal`
- `parado`

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:bus-operation-status",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "normal"
  }
}
```
