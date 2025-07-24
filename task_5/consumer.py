import sys
import nats
import json
import signal
import random
import asyncio
from producer import get_logger
from nats.js.api import ConsumerConfig


async def main(_from, _to, _name):
    nc = await nats.connect([
        'nats://localhost:4222', 
        'nats://localhost:4223', 
        'nats://localhost:4224'],
        user='user',
        password='user')
    js = nc.jetstream()
    logger = get_logger(_name)
    logger.info('Start consuming')
    sub = await js.pull_subscribe(
        subject="users.requests", 
        durable='workers',
        config=ConsumerConfig(
            ack_policy="explicit",
            ack_wait=30,
            max_ack_pending=1))
        
    user_id, msg = None, None
    while True: 
        try:
            msgs = await sub.fetch(batch=1)
            for msg in msgs:
                user_id = msg.headers['user_id']
                data = json.loads(msg.data.decode())
                logger.info(f"Took {user_id}")
                response = {'group': _name, **data}
                await asyncio.sleep(random.uniform(float(_from), float(_to)))
                await nc.publish(
                    msg.headers['reply_to'], 
                    json.dumps(response).encode(),
                    headers={'user_id': user_id})                
                await msg.ack()
        except nats.errors.TimeoutError:
            logger.info('Empty queue')
        except asyncio.CancelledError:
            logger.info(f'Graceful shutdown on {user_id=}')
            if msg and msg.is_acked is False:
                await msg.nak()
            await nc.close()
            break
        except Exception as exc:
            logger.exception(exc)
            await nc.close()
            raise exc


if __name__ == '__main__':
    _from, _to = sys.argv[1:3] if len(sys.argv) >= 3 else (2, 3)
    _name = sys.argv[3] if len(sys.argv) >= 4 else 'workers'
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)    
    main_task = loop.create_task(main(_from, _to, _name))
    signal.signal(signal.SIGINT, lambda a, b: main_task.cancel())
    loop.run_until_complete(main_task)
