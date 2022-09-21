import time
import logging
from paho.mqtt import client as paho_mqtt_client

logger = logging.getLogger('mqtt-client')
class MQTTClient:

    def __init__(self, client_id, broker, port, topic) -> None:
        self.mqttclient = paho_mqtt_client.Client(client_id)
        self.broker = broker
        self.port = port
        self.topic = topic

    def connect_mqtt(self):
        def on_connect(client, userdata, flags, rc) -> None:
            if rc == 0:
                print("Connected to MQTT Broker on topic %s", self.topic)
            else:
                print("Failed to connect to topic %s, return code %d\n", self.topic, rc)

        self.mqttclient.on_connect = on_connect
        self.mqttclient.connect(self.broker, self.port)
        return self.mqttclient

    def publish(self, msg):
        result = self.mqttclient.publish(self.topic, msg)
        status = result[0]
        if status == 0:
            print(f"Send `{msg}` to topic `{self.topic}`")
        else:
            print(f"Failed to send message to topic {self.topic}")

    def publish_loop(self):
        msg_count = 0
        #self.mqttclient.loop_start()
        while True:
            time.sleep(1)
            msg = f"messages: {msg_count}"
            result = self.mqttclient.publish(self.topic, msg)
            status = result[0]
            if status == 0:
                print(f"Send `{msg}` to topic `{self.topic}`")
            else:
                print(f"Failed to send message to topic {self.topic}")
            msg_count += 1

    def subscribe(self):
        def on_message(client, userdata, msg):
            print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        self.mqttclient.subscribe(self.topic)
        self.mqttclient.on_message = on_message
        self.mqttclient.loop_forever()