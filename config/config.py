import json
import os

class Config:
    def __init__(self):
        self.amqp_broker = os.environ.get('AMQP_BROKER',' http://localhost:5672/')
        self.elasticsearch_url = os.environ.get('ELASTICSEARCH_URL','https://localhost:9200/')
        self.elasticsearch_username = os.environ.get('ELASTICSEARCH_USERNME', 'elastic')
        self.elasticsearch_password = os.environ.get('ELASTICSEARCH_PASSWORD', 'oRDd*wYGG+28Zvd1bAW0')
        self.capability_period = os.environ.get('CAPABILITY_PERIOD', 5)

