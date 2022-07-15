from kafka.admin import KafkaAdminClient, NewTopic
from config import Configurations


class Topic:
    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.config = Configurations().get_kafka_topic_config(self.topic_name)
        kafka_options = self.config["kafka_options"]
        self.client = KafkaAdminClient(bootstrap_servers=[kafka_options["bootstrap.servers"]])
        self.create_topic_if_not_exists()

    def get_topics(self):
        """
        Retrieves current list of topics in kafka
        :return:
        """
        topics = self.client.list_topics()
        return topics

    def create_topic(self, topic_name):
        partitions = self.config["partitions"]
        rep_factor = self.config["replication_factor"]
        topic_list = [
            NewTopic(
                name=topic_name,
                num_partitions=partitions,
                replication_factor=rep_factor,
            )
        ]  # we generally create one topic
        # Call create_topics to asynchronously create topics. A dict of <topic, future> is returned.
        self.client.create_topics(topic_list)  # ret

        return {"topic": topic_name, "status": "success"}

    def create_topic_if_not_exists(self):
        """
        detail info about topic created
        :return: dict()
        """
        # topic naming conventions - <environment>.<customer>.<use-case>.<technology>.<level>.<source-db
        # example - prod.global.sync.snowflakes.l2.claims
        topic = self.config["topic_name"]
        available_topics = self.get_topics()

        if topic not in available_topics:
            resp = self.create_topic(topic)
        else:
            resp = {"topic": topic, "status": "success"}
        return resp

    def delete_topic(self, topic_name):
        """
        Deletes the topic
        :param topic_name:
        :return:
        """
        topics = [topic_name]
        available_topics = self.get_topics()
        if topic_name in available_topics:
            self.client.delete_topics(topics)
        else:
            raise Exception(f"No topic found with given name - {topic_name}")
