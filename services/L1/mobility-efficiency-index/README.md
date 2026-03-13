# mobility-efficiency-index

## Description

Combines traffic speed and bus operation status to infer a mobility efficiency indicator.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/traffic-speed`
- `service/mobility/inference/bus-operation-status`

## Output Topic

`service/mobility/inference/mobility-efficiency-index`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:mobility-efficiency-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "high"
  }
}
```
