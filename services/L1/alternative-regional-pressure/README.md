# alternative-regional-pressure

## Description

Alternative composition path for regional mobility pressure based on traffic flow and bus regional supply.

## Application Domain

mobility

## Vocabularies

IoT-Stream, SOSA/SSN, SAREF

## Input Topics

- `service/mobility/inference/traffic-flow`
- `service/mobility/inference/bus-regional-supply`

## Output Topic

`service/mobility/inference/alternative-regional-pressure`

## ML Model

Scikit-learn-compatible model loaded from `model.joblib` through the generic runtime (`run_generic_service`).

## Possible Outputs

- Model-dependent prediction label/value (stringified).
- Runtime fallback values: `model-not-loaded`, `inference-error`, `model-without-predict-method`.

## Output Example

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:alternative-regional-pressure",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "medium"
  }
}
```
