import sys
import nats
import json
import random
import asyncio
from asyncio import run
from datetime import datetime
from misc import SUBJECT, URL, get_logger


async def main(_from, _to, _group):
    nc = await nats.connect(URL)
    sub = await nc.subscribe(SUBJECT, queue=_group)
    logger = get_logger(_group)
    logger.info('Start consuming')

    async for msg in sub.messages:
        user_id = msg.headers['user_id']
        if int(msg.headers['ts']) < int(datetime.now().timestamp()) - 2:
            # оставляю 3 секунды на обработку сообщения. если ожидание уже 4, точно не успею
            logger.warning(f'Skipping {user_id=}')
            continue
        data = json.loads(msg.data.decode())
        logger.info(f"{user_id}, pending={sub.pending_msgs}")
        response = {'group': _group, **data}
        await asyncio.sleep(random.uniform(float(_from), float(_to)))
        await msg.respond(json.dumps(response).encode())


if __name__ == '__main__':
    _from, _to = sys.argv[1:3] if len(sys.argv) >= 3 else (2, 3)
    _group = sys.argv[3] if len(sys.argv) >= 4 else 'workers'
    run(main(_from, _to, _group))
