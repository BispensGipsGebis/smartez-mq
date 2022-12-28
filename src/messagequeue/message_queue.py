from dataclasses import dataclass
import pymongo
import os
from smartezlogger import logger

@dataclass
class MQ():
    def __init__(self):
        try:
            client = pymongo.MongoClient(
                os.environ["MONGODB_URL"])
            self.mongo_db = client
        except Exception as e:
            print(e)

    def producer(self, activity, user_id, params, unique = False):
        message: dict = {}
        message = {
            'activity': activity,
            'user_id': user_id,
            'params': params
        }
        if unique:
            result = self.mongo_db['smartez']['MQ'].find_one({'activity':activity, 'user_id':user_id, 'params':params} )
            if result:
                return True
        # saving to mongo
        result = self.mongo_db['smartez']['MQ'].insert_one(
            message)
        return message

    def consumer(self, topic, delete = False):
        ''' Interface for the consumer to get messages from the queue
         '''
        try:
            result = self.mongo_db['smartez']['MQ']
            for message in result.find():
                if message['activity'] == topic:
                    if delete:
                        self.delete_mq(message)
                    return message
        except Exception as e:
            logger.log_to_console(
                    'INFO', 'MQ::consumer', 'error consuming message:{}'.format(e))

    def delete_mq(self, message):
        try:
            result = self.mongo_db['smartez']['MQ'].delete_one({'_id':message['_id']})
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
            return True
        except Exception as e:
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'error deleting message:{}'.format(e))


