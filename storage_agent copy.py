from elasticsearch import Elasticsearch
import logging, json
from protocols.amqp.receive import Receiver
from protocols.amqp.send import Sender
from datetime import datetime
from threading import Thread, Event

class StorageAgent:
    def __init__(self, config):
        self.cfg = config
        self.es = self.create_elasticsearch_instance()
        self.sender = Sender()
        self.running_specs = {}  # Dictionary to store running specifications and their threads
    
    def send_receipt(self, event):
        receipt_msg=json.loads(event.message.body)
        receipt_msg['receipt'] = receipt_msg['specification']
        receipt_msg["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
        del receipt_msg['specification']
        topic_reply_to =  event.message.reply_to
        url = self.cfg.amqp_broker
        self.sender.send(url, topic = topic_reply_to, messages= receipt_msg)

    def subscribe_to_telemetry_service(self,endpoint):
        topic='topic://'+endpoint+'/specifications'
        url = self.cfg.amqp_broker
        logging.info("Agent will start lesstning for events from the telemetry service")
        receiver = Receiver(on_message_callback=self.telemetry_service_on_message_callback)
        receiver.receive_event(url,topic)

    


    def create_elasticsearch_instance(self):
        es_url = self.cfg.elasticsearch_url
        es = Elasticsearch(
            hosts=es_url,
            basic_auth=(self.cfg.elasticsearch_username, self.cfg.elasticsearch_password),
            verify_certs=False
        )
        return es

    def process_specification(self, specification_msg, interrupt_event):
        if interrupt_event.is_set():
            logging.info(f"Specification {specification_msg['specification']} interrupted.")
            spec_name = specification_msg['specification']
            unique_id = specification_msg['schema'] 
            key_to_delete = (spec_name, unique_id)
            if key_to_delete in self.running_specs:
                del self.running_specs[key_to_delete]
            return
        topic_to_subscribe = specification_msg["parameters"]["topic_to_subscribe"]
        logging.info("msg received {}".format(specification_msg))
        receiver = Receiver(on_message_callback=self.store_data_in_elasticsearch)
        url = self.cfg.amqp_broker
        topic_to_subscribe = 'topic://'+topic_to_subscribe
        receiver.receive_event(url,topic_to_subscribe)

    def telemetry_service_on_message_callback(self, event):
        specification_msg = json.loads(event.message.body)
        if "specification" in specification_msg:
            self.send_receipt(event)
            spec_name = specification_msg['specification']
            unique_id = specification_msg['schema'] 
            interrupt_event = Event()
            thread = Thread(target=self.process_specification, args=(specification_msg, interrupt_event))
            self.running_specs[(spec_name, unique_id)] = (thread, interrupt_event)
            thread.start()
        elif 'interrupt' in specification_msg:
            spec_name = specification_msg['interrupt']
            unique_id = specification_msg['schema']
            key = (spec_name, unique_id)
            if key in self.running_specs:
                _, interrupt_event = self.running_specs[key]
                interrupt_event.set()
                logging.info(f"Interrupt signal sent for specification {spec_name}")
            else:
                logging.warning(f"Specification {spec_name} not found.")
        else:
            logging.warning("Unknown message type.")

    def store_data_in_elasticsearch(self, event):
        # Attempt to load event.message.body as JSON
        data_msg = json.loads(event.message.body)
        ##########
        index_name = "tt_data"
        data = data_msg["resultValues"][0][0]
        # Index the data in Elasticsearch
        self.es.index(index=index_name, body=data)