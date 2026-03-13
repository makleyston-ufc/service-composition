# regional-mobility-pressure

## Description

Combines traffic flow and bus regional supply to infer regional mobility pressure.

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

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:regional-mobility-pressure",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "medium"
  }
}
```
