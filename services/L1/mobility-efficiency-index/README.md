# mobility-efficiency-index

## Descricao

Servico L1 que infere **eficiencia de mobilidade** combinando dois sinais ja inferidos na camada L0:

- velocidade de trafego (`traffic-speed`)
- status de operacao de onibus (`bus-operation-status`)

O servico consome os dois topicos MQTT, monta um vetor de features, executa um pipeline `scikit-learn` salvo em `model.joblib` e publica a classificacao final.

## Dominio

`mobility`

## Vocabularios semanticos

`IoT-Stream`, `SOSA/SSN`, `SAREF`

## Topicos MQTT

### Inputs

- `service/mobility/inference/traffic-speed`
- `service/mobility/inference/bus-operation-status`

### Output

- `service/mobility/inference/mobility-efficiency-index`

## Modelo de ML

- Artefato: `model.joblib`
- Tipo atual: `sklearn.pipeline.Pipeline`
- Estimador atual no pipeline: `RandomForestClassifier`
- Classes previstas: `baixa`, `media`, `alta`

Alem da classe, o servico tambem calcula:

- `urn:ufcity:confidence`: maior probabilidade entre as classes
- `urn:ufcity:efficiencyIndex`: indice continuo (0-100) derivado das probabilidades por classe

## Como o servico funciona (ciclo de inferencia)

1. Ao receber mensagem em um dos topicos de entrada, o servico faz parsing e guarda o **ultimo valor por topico**.
2. Enquanto nao existir pelo menos uma mensagem de **cada** topico, nada e publicado.
3. Quando ja existem os dois lados, **cada nova mensagem** em qualquer topico dispara uma nova inferencia.
4. A inferencia usa:
   - valor recem-chegado do topico atualizado
   - ultimo valor conhecido do outro topico
5. O resultado e publicado em `service/mobility/inference/mobility-efficiency-index`.

## Entradas em tempos/frequencias diferentes

Quando os inputs chegam em ritmos diferentes, o servico opera em modo **last-value cache** (ultimo valor conhecido), sem janela temporal:

- Se `traffic-speed` publicar 10 vezes antes de novo `bus-operation-status`, havera inferencias intermediarias combinando:
  - `traffic-speed` novo
  - `bus-operation-status` antigo (ultimo recebido)
- Quando `bus-operation-status` finalmente chegar, uma nova inferencia e disparada com esse valor novo.

Em outras palavras: o servico **nao espera sincronizacao estrita por timestamp**; ele sempre combina os ultimos estados disponiveis de cada fonte.

## Regras atuais e limitacoes

- Nao ha `timeout` de “staleness” (nao invalida automaticamente dado antigo).
- Nao ha janela de agregacao/event-time.
- Nao ha deduplicacao explicita por `@id`.
- Nao ha ordenacao temporal forcada para mensagens fora de ordem.
- O `timestamp` de saida e o maior entre os timestamps das duas mensagens atualmente em cache.

Se voce precisar de comportamento mais estrito (ex.: so inferir quando as duas entradas estiverem no mesmo intervalo de tempo), sera necessario adicionar politica de sincronizacao temporal no codigo.

## Variaveis de ambiente principais

- `MQTT_HOST` (default: `localhost`)
- `MQTT_PORT` (default: `1883`)
- `MQTT_CLIENT_ID` (default: `mobility-efficiency-index`)
- `MQTT_SUB_TOPICS` (default: os dois topicos de input acima, na ordem esperada)
- `MQTT_PUB_TOPIC` (default: topico de output acima)
- `SERVICE_URN` (default: `urn:ufcity:service:mobility-efficiency-index`)
- `SEMANTIC_PROPERTY_KIND` (default: `urn:ufcity:propertykind:MobilityEfficiencyIndex`)

## Exemplo de saida (resumido)

```json
{
  "@type": ["iot-stream:StreamObservation", "saref:Observation"],
  "iot-stream:belongsTo": "urn:ufcity:service:mobility-efficiency-index",
  "saref:hasResult": {
    "@type": "saref:PropertyValue",
    "saref:hasValue": "media",
    "urn:ufcity:confidence": 0.742311,
    "urn:ufcity:efficiencyIndex": 63.2
  },
  "urn:ufcity:payload": {
    "inputs_summary": {
      "traffic_status": "moderado",
      "bus_status": "lento"
    },
    "class_probabilities": {
      "baixa": 0.10,
      "media": 0.74,
      "alta": 0.16
    }
  }
}
```
