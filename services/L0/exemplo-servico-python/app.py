import os
import time

import paho.mqtt.client as mqtt

BROKER_HOST = os.getenv("MQTT_HOST", "mosquitto")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "l0-service-example")
PUB_TOPIC = os.getenv("MQTT_PUB_TOPIC", "services/l0/example/events")
SUB_TOPIC = os.getenv("MQTT_SUB_TOPIC", "services/l0/example/commands")


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[l0-example] conectado com rc={rc}")
    client.subscribe(SUB_TOPIC)


def on_message(client, userdata, msg):
    print(f"[l0-example] mensagem recebida em {msg.topic}: {msg.payload.decode(errors='ignore')}")


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=CLIENT_ID)
client.on_connect = on_connect
client.on_message = on_message

client.connect(BROKER_HOST, BROKER_PORT, keepalive=30)
client.loop_start()

seq = 0
while True:
    payload = f"l0_example_online seq={seq}"
    client.publish(PUB_TOPIC, payload)
    print(f"[l0-example] publicou: {payload}")
    seq += 1
    time.sleep(5)
