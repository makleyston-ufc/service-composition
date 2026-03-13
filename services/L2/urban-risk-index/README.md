# urban-risk-index

## Description

Infers urban infrastructure risk from urban congestion indicators.

## Application Domain

infrastructure

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/urban-congestion-index`

## Output Topic

`service/infrastructure/inference/urban-risk-index`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:urban-risk-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "high"
  }
}
```
