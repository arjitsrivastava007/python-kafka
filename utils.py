import os
from dotenv import load_dotenv
load_dotenv()


global_conf = {
    "kafka_bootstrap_servers": os.getenv(
        "bootstrap_servers", "localhost:9092"
    ),  # expected as comma seperated - "host:port,host1:port1,..."
    "kafka_sasl_username": os.getenv("sasl_plain_username", None),
    "kafka_sasl_password": os.getenv("sasl_plain_password", None),
    "kafka_sasl_mechanism": os.getenv("sasl_mechanism", "PLAIN"),
    "kafka_security_protocol": os.getenv("security_protocol", "SASL_PLAINTEXT"),
}
