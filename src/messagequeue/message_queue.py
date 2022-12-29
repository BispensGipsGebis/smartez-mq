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

    def producer(self, topic, activity, user_id, params, unique = False):
        ''' Interface for the producer to send messages to the queue
            Sequence:topic, activity '''
        # Issue: #1
        message: dict = {}
        message = {
            'topic': topic,
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

    def consumer(self, topic, delete = False, all = False):
        ''' Interface for the consumer to get messages from the queue
         '''
        try:
            result = self.mongo_db['smartez']['MQ']
            messages = []
            for message in result.find():
                if message.get('topic'):
                    if message['topic'] == topic:
                        if delete:
                            self.delete_mq(message)
                        if not all:
                            return message
                        else:
                            messages.append(message)
            if len(messages) > 0:
                return messages
            return None
        except Exception as e:
            logger.log_to_console(
                    'INFO', 'MQ::consumer', 'error consuming message:{}'.format(e))

    def delete_mq(self, message):
        try:
            self.mongo_db['smartez']['MQ'].delete_one({'_id':message['_id']})
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
            return True
        except Exception as e:
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'error deleting message:{}'.format(e))


