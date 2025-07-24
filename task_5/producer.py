import sys
import nats
import json
import signal
import random
import asyncio
import logging
from uuid import uuid4
from datetime import datetime
from functools import partial
from itertools import cycle
from asyncio import create_task
from nats.errors import TimeoutError
from nats.js.api import ConsumerConfig


def get_logger(name):
    LOG_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FMT))
    logger.addHandler(console_handler) 
    return logger


async def make_request(nc, js, _name, message, logger):
    user_id = str(message['user_id'])
    reply_to = f"users.responses.{user_id}"
    headers = dict(user_id=user_id, reply_to=reply_to)
    sub = None
    try:
        logger.info(f'Published {user_id=}')
        await nc.publish(
            'users.requests', 
            json.dumps(message).encode(),
            headers=headers)
        sub = await js.pull_subscribe(
            subject=reply_to, 
            durable='producers', 
            config=ConsumerConfig(
                durable_name='producers',
                ack_policy="explicit"))
        msg = (await sub.fetch(batch=1, timeout=5))[0]
        group = json.loads(msg.data).get('group')
        await msg.ack()
        logger.info(f"Received response for {user_id=}: {group=}") 
    except asyncio.CancelledError:
        logger.info(f"{user_id=} => 'user.on_broker_shutdown'")
        await nc.publish(
            'user.on_producer_shutdown', 
            json.dumps(message).encode(), 
            headers=headers)
    except TimeoutError:
        logger.error(f'Timeout for {user_id=}')


async def listen_to_other_producers_shutdown(is_cancelled, nc, js, _name, logger, tasks):
    try:
        sub = await nc.subscribe(
            'user.on_producer_shutdown', 
            queue='on_producer_shutdown')
        async for msg in sub.messages:
            task_id = str(uuid4())
            message = json.loads(msg.data.decode())
            user_id = message['user_id']
            logger.info(f"'user.on_broker_shutdown' => {user_id=}")
            tasks[task_id] = create_task(make_request(nc, js, _name, message, logger))
            rm_from_tasks = partial(lambda d, k, _: d.pop(k), tasks, task_id)
            tasks[task_id].add_done_callback(rm_from_tasks)
    except asyncio.CancelledError:
        await sub.unsubscribe()
        is_cancelled.set()
    except Exception as exc:
        logger.exception(exc)


async def main(_name, _multiplier):
    nc = await nats.connect([
        'nats://localhost:4222', 
        'nats://localhost:4223', 
        'nats://localhost:4224'],
        user='user',
        password='user')
    js = nc.jetstream()
    logger = get_logger(_name)
    users_bank = cycle(range(1000))
    tasks = dict()
    listen_cancelled = asyncio.Event()
    listen_task = create_task(listen_to_other_producers_shutdown(
        listen_cancelled, nc, js, _name, logger, tasks))
    try:
        while True:
            user_id = next(users_bank) * int(_multiplier)
            ts = str(int(datetime.now().timestamp()))
            task_id = str(uuid4())
            message = {"user_id": user_id, "ts": ts}
            tasks[task_id] = create_task(make_request(nc, js, _name, message, logger))
            rm_from_tasks = partial(lambda d, k, _: d.pop(k), tasks, task_id)
            tasks[task_id].add_done_callback(rm_from_tasks)
            await asyncio.sleep(random.uniform(0, 1.5))
    except asyncio.CancelledError:
        listen_task.cancel()
        logger.info('Graceful shutdown')
        # нельзя продолжать без отписки от on_producer_shutdown, иначе сообщения вернутся
        await listen_cancelled.wait()
        for task in tasks.values():
            task.cancel()
        while len(tasks) > 0:
            await asyncio.sleep(0.1)
    finally:
        await nc.close()


if __name__ == '__main__':
    _name = sys.argv[1] if len(sys.argv) >= 2 else 'producer'
    _multiplier = sys.argv[2] if len(sys.argv) == 3 else 1
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)    
    main_task = loop.create_task(main(_name, _multiplier))
    signal.signal(signal.SIGINT, lambda a, b: main_task.cancel())
    loop.run_until_complete(main_task)
