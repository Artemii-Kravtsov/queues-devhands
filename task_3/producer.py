import logging
import sys
from functools import partial
from confluent_kafka import Producer
from threading import Thread
import json
import time
import random


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(name)s | %(message)s"))
    logger.addHandler(console_handler) 
    return logger


def delivery_report(logger, err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        value = json.loads(msg.value())
        if msg.topic() == 'purchases':
            log = f'{value["user_id"]}: amount={value["amount"]}{", late!" if value["is_late"] else ""}'
        else:
            log = f'{value["user_id"]}: {value["event_type"]}'
        logger.info(log)


def produce_purchase(purchase_logger, producer, event_type, user_id, page_id, timestamp):
    amount = round(random.random() * 5000)
    is_late = random.random() < 0.2
    offset = (random.randint(0, 4) + (is_late * random.randint(5, 10))) * random.choice([1, -1])
    event = dict(
        user_id=user_id, 
        page_id=page_id, 
        amount=amount,
        is_late=is_late,  # чтобы визуально было проще понять по логам, что к чему
        timestamp=timestamp + offset)
    if offset > 0:
        time.sleep(offset)
    producer.produce(
        'purchases',
        key=str(event['user_id']),
        value=json.dumps(event),
        callback=partial(delivery_report, purchase_logger))


def main(producer, purchase_logger, events_logger):
    while True:
        event_type = random.choice([*(['page-view'] * 2), *(['click'] * 2), 'purchase'])
        event = {
            'event_type': event_type,
            'user_id': random.randint(1, 20),
            'page_id': random.randint(1, 100),
            'timestamp': int(time.time())}
        if event_type == 'purchase':
            Thread(target=produce_purchase, args=(purchase_logger, producer), kwargs=event).start()
        producer.produce(
            'user-events',
            key=str(event['user_id']),
            value=json.dumps(event),
            callback=partial(delivery_report, events_logger))
        producer.poll(0)
        time.sleep(random.uniform(0.5, 3))


if __name__ == '__main__':
    acks = sys.argv[1] if len(sys.argv) > 1 else '0'
    producer_config = {
        'bootstrap.servers': 'localhost:19090,localhost:19091,localhost:19092',
        'acks': acks}
    print(producer_config)
    producer = Producer(producer_config)
    purchase_logger = get_logger('purchase   ')
    events_logger = get_logger('user-events')    
    try:
        main(producer, purchase_logger, events_logger)
    except KeyboardInterrupt:
        purchase_logger.info('Preparing for shutdown')
        producer.flush()
