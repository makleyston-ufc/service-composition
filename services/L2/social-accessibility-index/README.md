# social-accessibility-index

## Description

Combines mobility pressure and mobility efficiency to infer social accessibility.

## Application Domain

social

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/regional-mobility-pressure`
- `service/mobility/inference/mobility-efficiency-index`

## Output Topic

`service/social/inference/social-accessibility-index`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:social-accessibility-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "low"
  }
}
```
