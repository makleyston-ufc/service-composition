# traffic-flow

## Description

Infers traffic flow from radar telemetry observations.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/observation/radar-telemetry`

## Output Topic

- `service/mobility/inference/traffic-flow`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:traffic-flow",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "high"
  }
}
```
