import json
import os, logging

class Config:
    def __init__(self):
        self.amqp_broker = os.environ.get('AMQP_BROKER',' http://localhost:5672/')
        self.controller_amqp_port = os.environ.get('CONTROLLER_AMQP_PORT','5672')
        self.elasticsearch_url = os.environ.get('ELASTICSEARCH_URL','https://localhost:9200/')
        self.elasticsearch_username = os.environ.get('ELASTICSEARCH_USERNME', 'elastic')
        self.elasticsearch_password = os.environ.get('ELASTICSEARCH_PASSWORD', 'oRDd*wYGG+28Zvd1bAW0')
        self.config_file_path = os.environ.get('CONFIG', 'config/config.json')
        self.load_config_data()

    def load_config_data(self):
        with open(self.config_file_path, 'r') as f:
            config = json.load(f)
            self.amqp_storagae_events_topic = config.get("AMQP_STORAGE_EVENTS_TOPIC")
            self.capability_period = config.get("CAPABILITY_PERIOD")

