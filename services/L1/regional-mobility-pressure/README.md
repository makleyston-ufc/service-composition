# regional-mobility-pressure

## Descrição

Serviço L1 que infere **pressão regional de mobilidade** combinando dois sinais já inferidos na camada L0:

- fluxo de tráfego (`traffic-flow`)
- oferta regional de ônibus (`bus-regional-supply`)

O serviço consome os dois tópicos MQTT, monta um vetor de features, executa um pipeline `scikit-learn` salvo em `model.joblib` e publica a classificação final.

## Domínio

`mobility`

## Vocabulários semânticos

`IoT-Stream`, `SOSA/SSN`, `SAREF`

## Tópicos MQTT

### Inputs

- `service/mobility/inference/traffic-flow`
- `service/mobility/inference/bus-regional-supply`

### Output

- `service/mobility/inference/regional-mobility-pressure`

## Modelo de ML

- Artefato: `model.joblib`
- Tipo atual: `sklearn.pipeline.Pipeline`
- Estimador atual no pipeline: `RandomForestClassifier`
- Classes previstas: `baixa`, `media`, `alta`

Além da classe, o serviço também calcula:

- `urn:ufcity:confidence`: maior probabilidade entre as classes
- `urn:ufcity:pressureIndex`: índice contínuo (0-100) derivado das probabilidades por classe

## Como o serviço funciona (ciclo de inferência)

1. Ao receber mensagem em um dos tópicos de entrada, o serviço faz parsing e guarda o **último valor por tópico**.
2. Enquanto não existir pelo menos uma mensagem de **cada** tópico, nada é publicado.
3. Quando já existem os dois lados, **cada nova mensagem** em qualquer tópico dispara uma nova inferência.
4. A inferência usa:
   - valor recém-chegado do tópico atualizado
   - último valor conhecido do outro tópico
5. O resultado é publicado em `service/mobility/inference/regional-mobility-pressure`.

## Entradas em tempos/frequências diferentes

Quando os inputs chegam em ritmos diferentes, o serviço opera em modo **last-value cache** (último valor conhecido), sem janela temporal:

- Se `traffic-flow` publicar 10 vezes antes de novo `bus-regional-supply`, haverá inferências intermediárias combinando:
  - `traffic-flow` novo
  - `bus-regional-supply` antigo (último recebido)
- Quando `bus-regional-supply` finalmente chegar, uma nova inferência é disparada com esse valor novo.

Em outras palavras: o serviço **não espera sincronização estrita por timestamp**; ele sempre combina os últimos estados disponíveis de cada fonte.

## Regras atuais e limitações

- Não há `timeout` de “staleness” (não invalida automaticamente dado antigo).
- Não há janela de agregação/event-time.
- Não há deduplicação explícita por `@id`.
- Não há ordenação temporal forçada para mensagens fora de ordem.
- O `timestamp` de saída é o maior entre os timestamps das duas mensagens atualmente em cache.

Se você precisar de comportamento mais estrito (ex.: só inferir quando as duas entradas estiverem no mesmo intervalo de tempo), será necessário adicionar política de sincronização temporal no código.

## Variáveis de ambiente principais

- `MQTT_HOST` (default: `localhost`)
- `MQTT_PORT` (default: `1883`)
- `MQTT_CLIENT_ID` (default: `l1-regional-mobility-pressure`)
- `MQTT_SUB_TOPICS` (default: os dois tópicos de input acima, na ordem esperada)
- `MQTT_PUB_TOPIC` (default: tópico de output acima)
- `SERVICE_URN` (default: `urn:ufcity:service:regional-mobility-pressure`)
- `SEMANTIC_PROPERTY_KIND` (default: `urn:ufcity:propertykind:RegionalMobilityPressure`)

## Exemplo de saída (resumido)

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:regional-mobility-pressure",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:confidence": 0.742311,
    "urn:ufcity:pressureIndex": 65.8
  },
  "urn:ufcity:payload": {
    "inputs_summary": {
      "traffic_level": "alta",
      "bus_supply_level": "media"
    },
    "class_probabilities": {
      "baixa": 0.05,
      "media": 0.70,
      "alta": 0.25
    }
  }
}
```
