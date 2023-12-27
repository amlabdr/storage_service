#standards imports
import json, traceback, logging, re, time
from datetime import datetime
from threading import Thread

#imports to use AMQP 1.0 communication protocol
from proton.handlers import MessagingHandler
from proton.reactor import Container
from .send import Sender

sender = Sender()


class Receiver():
    def __init__(self):
        super(Receiver, self).__init__()
        
    def receive_event(self, server, topic, storage_agent):
        print("will start the rcv")
        Container(event_Receiver_handller(server,topic,storage_agent)).run()


class event_Receiver_handller(MessagingHandler):
    def __init__(self, server,topic,storage_agent):
        super(event_Receiver_handller, self).__init__()
        self.server = server
        self.topic = topic
        self.storage_agent = storage_agent
        logging.info("Agent will start listning for events in the topic: {}".format(self.topic))
        

    def on_start(self, event):
        conn = event.container.connect(self.server)
        event.container.create_receiver(conn, self.topic)

    def process_event(self, event_msg):
        index_name = event_msg["parameters"]["index_name"]
        data = event_msg["parameters"]["data"]
        status = self.storage_agent.store_data_in_elasticsearch(index_name, data)

    def on_message(self, event):
        try:
            jsonData = json.loads(event.message.body)
            logging.info("msg received {}".format(jsonData))
            status = self.process_event(jsonData)
            topic='topic://'+'topology.status'
            status_msg = {}
            status_msg["resourceStatus"]=status
            print("status msg",status_msg)
            sender.send(self.server,topic, status_msg)

            
        except Exception:
            traceback.print_exc()
            