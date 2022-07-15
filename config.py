from utils import global_conf
import os
from dotenv import load_dotenv
load_dotenv()


class Configurations:

    @staticmethod
    def get_kafka_topic_config(kafka_topic_name):
        """
        takes inputs form payload and config and form new config
        :return:
        """
        kafka_config = {
            "topic_name": kafka_topic_name,
            "partitions": 1,
            "replication_factor": 1,
            "kafka_options": {},
        }

        # fill it using SASL Authentication params
        kafka_config["kafka_options"]["bootstrap.servers"] = global_conf[
            "kafka_bootstrap_servers"
        ]

        if global_conf["kafka_sasl_username"]:
            kafka_config["kafka_options"]["sasl.username"] = global_conf[
                "kafka_sasl_username"
            ]
            kafka_config["kafka_options"]["sasl.password"] = global_conf[
                "kafka_sasl_password"
            ]
            kafka_config["kafka_options"]["sasl.mechanism"] = global_conf[
                "kafka_sasl_mechanism"
            ]
            kafka_config["kafka_options"]["security.protocol"] = global_conf[
                "kafka_security_protocol"
            ]

        return kafka_config

    @staticmethod
    def get_kafka_producer_config():
        kafka_config = {}
        kafka_config["bootstrap_servers"] = global_conf["kafka_bootstrap_servers"]
        if global_conf["kafka_sasl_username"]:
            kafka_config["sasl_plain_username"] = global_conf["kafka_sasl_username"]
            kafka_config["sasl_plain_password"] = global_conf["kafka_sasl_password"]
            kafka_config["sasl_mechanism"] = global_conf["kafka_sasl_mechanism"]
            kafka_config["security_protocol"] = global_conf["kafka_security_protocol"]

        return kafka_config

    @staticmethod
    def get_kafka_consumer_config():
        kafka_config = {
            "auto_offset_reset": "earliest",
            "enable_auto_commit": True,
            "max_in_flight_requests_per_connection": int(os.environ.get("MAX_REQUESTS", 5)),
            "max_poll_records": int(os.environ.get("MAX_RECORDS", 100)),
            "max_poll_interval_ms": int(os.environ.get("MAX_RECORDS", 1000)),
            "bootstrap_servers": global_conf["kafka_bootstrap_servers"]
        }

        if global_conf["kafka_sasl_username"]:
            kafka_config["sasl_plain_username"] = global_conf["kafka_sasl_username"]
            kafka_config["sasl_plain_password"] = global_conf["kafka_sasl_password"]
            kafka_config["sasl_mechanism"] = global_conf["kafka_sasl_mechanism"]
            kafka_config["security_protocol"] = global_conf["kafka_security_protocol"]

        return kafka_config
