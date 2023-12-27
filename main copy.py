import pika
from elasticsearch import Elasticsearch
import json, yaml
from config.config import Config
from controller.controller_service import Controller_service

INDEX_NAME = "data_index"

def read_config_from_yaml(file_path='config.yaml'):
    with open(file_path, 'r') as file:
        config_data = yaml.safe_load(file)
    return config_data

def create_elasticsearch_instance(config):
    es_url = f"{config['elasticsearch']['scheme']}://{config['elasticsearch']['host']}:{config['elasticsearch']['port']}"
    es = Elasticsearch(hosts=es_url,
                       basic_auth=(config['elasticsearch']['username'], config['elasticsearch']['password']),
                       verify_certs=False
                       )
    return es

def store_data_in_elasticsearch(data):
    # Index the data in Elasticsearch
    es.index(index=INDEX_NAME, body=data)


def start_storage_agent():
    cfg = Config()
    Ctl_service = Controller_service(config = cfg)
    Ctl_service.subscribe_to_events()
    # Simulate receiving a message
    message_data = {
        "name": "x"
        # Add other fields as needed
    }

    # Simulate processing the message
    print(f"Simulating message processing: {message_data}")

    # Store data in Elasticsearch
    store_data_in_elasticsearch(message_data)

if __name__ == '__main__':
    # Read configuration from YAML file
    config = read_config_from_yaml()

    # Create Elasticsearch instance
    es = create_elasticsearch_instance(config)
    # Test the connection
    try:
        info = es.info()
        print(f"Connected to Elasticsearch: {info}")
    except Exception as e:
        print(f"Failed to connect to Elasticsearch: {e}")
    start_storage_agent()
    # Index name
    index_name = "data_index"

    # Search query (match all documents in this example)
    search_query = {
        "query": {
            "match_all": {}
        }
    }

    # Perform the search
    search_results = es.search(index=index_name, body=search_query)

    # Print the results
    for hit in search_results["hits"]["hits"]:
        print(hit["_source"])
