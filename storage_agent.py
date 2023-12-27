from elasticsearch import Elasticsearch

class StorageAgent:
    def __init__(self, config):
        self.cfg = config
        self.es = self.create_elasticsearch_instance()

    def create_elasticsearch_instance(self):
        es_url = self.cfg.elasticsearch_url
        es = Elasticsearch(
            hosts=es_url,
            basic_auth=(self.cfg.elasticsearch_username, self.cfg.elasticsearch_password),
            verify_certs=False
        )
        return es

    def store_data_in_elasticsearch(self, index_name, data):
        # Index the data in Elasticsearch
        self.es.index(index=index_name, body=data)