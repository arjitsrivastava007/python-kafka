# Kafka Producer Consumer Example
This project focuses on setting up Kafka locally and sending message via producer to consumer using kafka-python package

## Setup Virtual Environment
- Make sure the python version is 3
- `python -m venv venv`
- `source venv/bin/activate`
- `pip install -U pip`

## Install Requirements
- `pip install -r requirements.txt`

## Star the kafka server
- Download kafka 3.2.0 from https://dlcdn.apache.org/kafka/3.2.0/kafka-3.2.0-src.tgz
- `tar -xzf kafka-3.2.0-src.tgz`
- `cd kafka-3.2.0-src`
- Start the zookeeper server: `bin/zookeeper-server-start.sh config/zookeeper.properties`
- Start the Kafka server: `bin/kafka-server-start.sh config/server.properties`

## Execution
- `python main.py`
