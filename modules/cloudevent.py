import requests
from cloudevents.http import CloudEvent, from_http, to_binary

class CloudEventService():

    def __init__(self):
        self.source = None

    def build_attributes(self, type, source):
        attributes = {
            "type" : type,
            "source" : source
        }
        return attributes

    def send_message(self, target_address, source_address, message_type, data):
        attributes = self.build_attributes(message_type, source_address)

        event = CloudEvent(attributes, data)
        headers, body = to_binary(event)

        requests.post(target_address, headers=headers, data=body)
        print(f"Sent {event['id']} from {event['source']} with " f"{event.data}")

    def receive_message(self, request):
        return from_http(request.headers, request.get_data())