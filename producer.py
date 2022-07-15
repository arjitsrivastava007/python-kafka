from json import dumps
from kafka import KafkaProducer
from config import Configurations


class Producer:
    def __init__(self):
        self.producer = None
        self.create()

    def create(self):
        kafka_producer_config = Configurations().get_kafka_producer_config()
        self.producer = KafkaProducer(
            **kafka_producer_config,
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    def send(self, topic, message):
        self.producer.send(topic, value=message)

