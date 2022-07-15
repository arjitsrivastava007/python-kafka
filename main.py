from time import sleep
from producer import Producer
from consumer import Consumer
from datetime import datetime
from topics import Topic
import os
from dotenv import load_dotenv
load_dotenv()


def main():
    # Create topic
    topic = Topic(os.environ.get("KAFKA_TOPIC", "audit_service"))

    try:
        # Create Producer
        producer = Producer()
        # Send messages to topic
        for i in range(1000):
            message = {"status": "ok", "message": "audit-log-event", "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), "number": i}
            producer.send(topic.topic_name, message)

        sleep(5)

        # Create consumer
        consumer = Consumer()
        # read consumer messages
        messages = consumer.messages(topic.topic_name)

        counter = 0
        for message in messages:
            counter += 1
            message = message.value
            print(message)

            if counter == 100:
                consumer.pause()
                sleep(5)
                consumer.resume()
                counter = 0
    except Exception as err:
        import traceback
        traceback.print_exc()
    finally:
        topic.delete_topic(os.environ.get("KAFKA_TOPIC", "audit_service"))


main()
