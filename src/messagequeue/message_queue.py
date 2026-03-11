from dataclasses import dataclass
import pymongo
import os
from smartezlogger import logger

@dataclass
class MQ():
    def __init__(self):
        self.mongo_db = None
        self._mongo_error = None
        try:
            client = pymongo.MongoClient(
                os.environ["MONGODB_URL"])
            self.mongo_db = client
        except Exception as e:
            self._mongo_error = e
            logger.log_to_console('ERROR', 'MQ::__init__', f'failed to initialize MongoDB client: {e}')

    def _mq_collection(self):
        if self.mongo_db is None:
            if self._mongo_error is not None:
                raise RuntimeError(f'MongoDB client is not initialized: {self._mongo_error}')
            raise RuntimeError('MongoDB client is not initialized')
        return self.mongo_db['smartez']['MQ']

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
            result = self._mq_collection().find_one({'activity':activity, 'user_id':user_id, 'params':params} )
            if result:
                return True
        # saving to mongo
        result = self._mq_collection().insert_one(
            message)
        return message

    def consumer(self, topic, delete=False, all=False):
        ''' Interface for the consumer to get messages from the queue
         '''
        try:
            result = self._mq_collection()
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
                    'ERROR', 'MQ::consumer', 'error consuming message:{}'.format(e))

    def delete_mq(self, message):
        try:
            self._mq_collection().delete_one({'_id':message['_id']})
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
            return True
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::delete_mq', 'error deleting message:{}, with error: {}'.format(message,e))


