import sys
import logging

SUBJECT = 'user.requests'
URL = 'nats://localhost:4222'
LOG_FMT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LOG_FMT))
    logger.addHandler(console_handler) 
    return logger
