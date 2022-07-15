from config import Configurations
from json import loads
from kafka import KafkaConsumer


class Consumer:
    def __init__(self):
        self.consumer = None

    def get(self, topic):
        """
        Purpose of this function is to initialize consumer with current topic_name
        :param topic:
        :return:
        """
        kafka_consumer_config = Configurations().get_kafka_consumer_config()
        self.consumer = KafkaConsumer(
            topic,
            **kafka_consumer_config,
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    def messages(self, topic):
        self.get(topic)
        return self.consumer

    def pause(self):
        """
        Pauses consumer from receiving further messages
        :return:
        """
        self.consumer.pause()

    def resume(self):
        """
        Resumes consumer from receiving further messages
        :return:
        """
        self.consumer.resume()
