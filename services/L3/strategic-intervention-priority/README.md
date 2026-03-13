# strategic-intervention-priority

## Description

Combines social accessibility and regional mobility pressure to infer strategic intervention priority.

## Application Domain

planning

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/social/inference/social-accessibility-index`
- `service/mobility/inference/regional-mobility-pressure`

## Output Topic

`service/planning/inference/strategic-intervention-priority`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:strategic-intervention-priority",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "high"
  }
}
```
