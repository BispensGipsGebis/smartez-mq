from operator import truediv
from message_queue import *
from smartezlogger import logger
import time

if __name__ == "__main__":
    try:
        print(dir(logger))
        logger.log_to_console(
            'INFO', 'mq_init', 'Initializing message queue')
        mq = MQ()
        mq.producer('test', 'test', 'test','test')
        while True:
            message = mq.consumer('test', True)
            time.sleep(1)
    except Exception as e:
        print(e)
