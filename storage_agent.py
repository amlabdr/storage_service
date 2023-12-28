from elasticsearch import Elasticsearch
import logging, json
from protocols.amqp.receive import Receiver
from protocols.amqp.send import Sender
from datetime import datetime
class StorageAgent:
    def __init__(self, config):
        self.cfg = config
        self.es = self.create_elasticsearch_instance()
        self.sender = Sender()
    

    def send_receipt(self, event):
        receipt_msg=json.loads(event.message.body)
        receipt_msg['receipt'] = receipt_msg['specification']
        receipt_msg["timestamp"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
        del receipt_msg['specification']
        topic_reply_to =  event.message.reply_to
        url = self.cfg.amqp_broker +":"+ self.cfg.controller_amqp_port
        self.sender.send(url, topic = topic_reply_to, messages= receipt_msg)

    def subscribe_to_telemetry_service(self,endpoint):
        topic='topic://'+endpoint+'/specifications'
        url = self.cfg.amqp_broker +":"+ self.cfg.controller_amqp_port
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

    def telemetry_service_on_message_callback(self, event):
        self.send_receipt(event)
        specification_msg = json.loads(event.message.body)
        topic_to_subscribe = specification_msg["parameters"]["topic_to_subscribe"]
        logging.info("msg received {}".format(specification_msg))
        receiver = Receiver(on_message_callback=self.store_data_in_elasticsearch)
        url = self.cfg.amqp_broker +":"+ self.cfg.controller_amqp_port
        topic_to_subscribe = 'topic://'+topic_to_subscribe
        receiver.receive_event(url,topic_to_subscribe)

    def store_data_in_elasticsearch(self, event):
        data_msg = json.loads(event.message.body)
        index_name = data_msg["parameters"]["index_name"]
        data = data_msg["parameters"]["data"]
        # Index the data in Elasticsearch
        self.es.index(index=index_name, body=data)