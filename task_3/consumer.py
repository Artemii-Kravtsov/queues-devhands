import sys
import logging
from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import time


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(name)s | %(message)s"))
    logger.addHandler(console_handler) 
    return logger


def main(consumer, loggers):
    try:
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
                if msg.topic() == 'visits':
                    log_event = dict(
                        user_id=event['user_id'], 
                        session_id=event['session_id'], 
                        event_type=event['event_type'],
                        amount=event['amount'],
                        timestamp=event['timestamp'])
                elif msg.topic() == 'suspicious-visits':
                    log_event = dict(
                        user_id=event['user_id'], 
                        session_id=event['session_id'],
                        type=event['type'])
                loggers[msg.topic()].info(str(log_event))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    offsets_policy = sys.argv[1] if len(sys.argv) > 1 else 'earliest'
    consumer_config = {
        'bootstrap.servers': 'localhost:19090,localhost:19091,localhost:19092',
        'group.id': 'analytics-group',
        'auto.offset.reset': offsets_policy}
    print(consumer_config)
    consumer = Consumer(consumer_config)
    consumer.subscribe(['visits', 'suspicious-visits', 'late-purchases'])
    loggers = {
        'visits': get_logger('visits'),
        'suspicious-visits': get_logger('suspicious-visits'),
        'late-purchases': get_logger('late-purchases')}
    main(consumer, loggers)

