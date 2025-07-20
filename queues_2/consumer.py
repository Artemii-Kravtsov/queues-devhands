import sys
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import time


def print_aggregates(event_counts, distinct_users):
    print(f"Event Counts: {event_counts}")
    print(f"Distinct Users: {len(distinct_users)}")

def main(consumer):
    event_counts = {
        'page-view': 0,
        'click': 0,
        'purchase': 0}
    distinct_users = set()
    try:
        start_time = time.time()
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f'End of partition reached {msg.topic()} [{msg.partition()}]')
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                event = json.loads(msg.value().decode('utf-8'))
                event_type = event.get('event_type')
                user_id = event.get('user_id')
                if event_type in event_counts:
                    event_counts[event_type] += 1
                if user_id is not None:
                    distinct_users.add(user_id)

            if time.time() - start_time > 3:
                print_aggregates(event_counts, distinct_users)
                start_time = time.time()
                event_counts = {key: 0 for key in event_counts}
                distinct_users.clear()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    offsets_policy = sys.argv[1] if len(sys.argv) > 1 else 'earliest'
    consumer_config = {
        'bootstrap.servers': 'localhost:19090,localhost:19091',
        'group.id': 'analytics-group-new',
        'auto.offset.reset': offsets_policy}
    print(consumer_config)
    consumer = Consumer(consumer_config)
    consumer.subscribe(['user-events'])
    main(consumer)

