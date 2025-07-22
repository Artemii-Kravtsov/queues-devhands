import sys
import nats
import json
import random
from datetime import datetime
import asyncio
from itertools import cycle
from asyncio import create_task, Event, sleep, run
from nats.errors import NoRespondersError, TimeoutError
from misc import SUBJECT, URL, get_logger


async def get_request(nc, message, no_consumers_event, logger):
    user_id = str(message['user_id'])
    ts = str(int(datetime.now().timestamp()))
    headers = dict(user_id=user_id, ts=ts)
    try:
        logger.info(f'Published {user_id=}')
        response = await nc.request(
            SUBJECT, 
            json.dumps(message).encode(),
            timeout=5, 
            headers=headers)
        group = json.loads(response.data).get('group')
    except NoRespondersError:
        no_consumers_event.set()
    except TimeoutError:
        logger.error(f'Timeout for {user_id=}')


async def main(_group, _multiplier):
    nc = await nats.connect(URL)
    no_consumers_event = Event()
    logger = get_logger(_group)
    users_bank = cycle(range(1000))
    try:
        while True:
            if no_consumers_event.is_set():
                logger.warning('No consumers available. Falling asleep')
                await sleep(10)
                no_consumers_event.clear()
            user_id = next(users_bank) * int(_multiplier)
            message = {"user_id": user_id}
            create_task(get_request(nc, message, no_consumers_event, logger))
            await asyncio.sleep(random.uniform(0, 1.5))
    finally:
        await nc.close()

if __name__ == '__main__':
    _group = sys.argv[1] if len(sys.argv) >= 2 else 'producer'
    _multiplier = sys.argv[2] if len(sys.argv) == 3 else 1
    run(main(_group, _multiplier))