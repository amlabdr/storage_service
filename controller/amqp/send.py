import json
import logging
from proton import Message
from proton.handlers import MessagingHandler
from proton.reactor import Container

class Sender:
    def __init__(self):
        pass

    def send(self, server, topic, messages):
        container = Container(SendHandler(server, topic, messages))
        container.run()

class SendHandler(MessagingHandler):
    def __init__(self, server, topic, messages):
        super(SendHandler, self).__init__()
        self.server = server
        self.topic = topic
        self.messages = messages
        self.confirmed = 0
        self.total = len(messages)

    def on_connection_error(self, event):
        logging.error(f"Connection error while sending messages to server: {self.server} for topic: {self.topic}")
        return super().on_connection_error(event)

    def on_transport_error(self, event):
        logging.error(f"Transport error while sending messages to server: {self.server} for topic: {self.topic}")
        return super().on_transport_error(event)

    def on_start(self, event):
        conn = event.container.connect(self.server)
        event.container.create_sender(conn, self.topic)

    def on_sendable(self, event):
        logging.info(f"Agent sending messages to topic {self.topic}")
        for message_data in self.messages:
            msg = Message(body=json.dumps(message_data))
            event.sender.send(msg)
        event.sender.close()

    def on_rejected(self, event):
        logging.error(f"Message rejected while sending messages to server: {self.server} for topic: {self.topic}")
        return super().on_rejected(event)

    def on_accepted(self, event):
        logging.info(f"Message accepted in topic {self.topic}")
        self.confirmed += 1
        if self.confirmed == self.total:
            event.connection.close()

    def on_disconnected(self, event):
        logging.error(f"Disconnected error while sending messages to server: {self.server} for topic: {self.topic}")
        self.sent = self.confirmed
