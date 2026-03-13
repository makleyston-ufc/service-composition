# urban-congestion-index

## Description

Combines traffic flow and traffic speed to infer an urban congestion indicator.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/traffic-flow`
- `service/mobility/inference/traffic-speed`

## Output Topic

`service/mobility/inference/urban-congestion-index`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:urban-congestion-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "critical"
  }
}
```
