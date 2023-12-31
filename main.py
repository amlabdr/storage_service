from config.config import Config
from storage_agent import StorageAgent
from threading import Thread
import json
import datetime
import logging
import traceback
import time
from protocols.amqp.send import Sender

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def start_storage_agent():
    # Load configuration
    cfg = Config()

    storage_agent = StorageAgent(config=cfg)

    with open("capability/storaage_capability.json", 'r') as capability_file:
        capability_data = json.load(capability_file)

    capability_data["timestamp"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
    topic = 'topic://'+'/capabilities'
    thread_send_capability = Thread(target=send_capability, args=(cfg.amqp_broker, topic, cfg.capability_period, capability_data))
    #thread_send_capability.start()
    endpoint = capability_data['endpoint']
    # Subscribe the ControllerService to events and pass the StorageAgent
    storage_agent.subscribe_to_telemetry_service(endpoint=endpoint)

def send_capability(url, topic, period, capability_data):
    while True:
        # Publish Capability in "/capabilities"
        try:
            capability_sender.send(url, topic, capability_data)
            logging.info('Capability sent')
        except Exception as e:
            logging.error("Agent can't send capability to the controller. Traceback:")
            logging.error(traceback.format_exc())
        time.sleep(period)

if __name__ == '__main__':
    # Start the storage agent
    capability_sender = Sender()
    start_storage_agent()
