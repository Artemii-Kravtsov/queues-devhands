import sys
from confluent_kafka import Producer
import json
import time
import random


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def main(producer):
    while True:
        event = {
            'event_type': random.choice(['page-view', 'click', 'purchase']),
            'user_id': random.randint(1, 1000),
            'page_id': random.randint(1, 100),
            'timestamp': time.time()}
        producer.produce(
            'user-events',
            key=str(event['user_id']),
            value=json.dumps(event),
            callback=delivery_report)
        producer.poll(0)
        time.sleep(random.uniform(0.1, 0.7))


if __name__ == '__main__':
    acks = sys.argv[1] if len(sys.argv) > 1 else '0'
    producer_config = {
        'bootstrap.servers': 'localhost:19090,localhost:19091,localhost:19092',
        'acks': acks}
    print(producer_config)
    main(Producer(producer_config))
