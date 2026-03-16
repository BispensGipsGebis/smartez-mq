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
            self._ensure_indexes()
        except Exception as e:
            self._mongo_error = e
            logger.log_to_console('ERROR', 'MQ::__init__', f'failed to initialize MongoDB client: {e}')

    def _mq_collection(self):
        if self.mongo_db is None:
            if self._mongo_error is not None:
                raise RuntimeError(f'MongoDB client is not initialized: {self._mongo_error}')
            raise RuntimeError('MongoDB client is not initialized')
        return self.mongo_db['smartez']['MQ']

    def _ensure_indexes(self):
        try:
            collection = self._mq_collection()
            collection.create_index([('topic', 1), ('_id', 1)])
            collection.create_index([('topic', 1), ('activity', 1), ('_id', 1)])
            collection.create_index([('activity', 1), ('user_id', 1)])
        except Exception as e:
            logger.log_to_console('WARNING', 'MQ::_ensure_indexes', 'failed to ensure MQ indexes: {}'.format(e))

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

    def consumer(self, topic, delete=False, all=False, activity=None, limit=None):
        ''' Interface for the consumer to get messages from the queue
         '''
        try:
            query = {'topic': topic}
            if activity is not None:
                query['activity'] = activity

            collection = self._mq_collection()
            if not all:
                message = collection.find_one(query, sort=[('_id', pymongo.ASCENDING)])
                if message and delete:
                    self.delete_mq(message)
                return message

            cursor = collection.find(query).sort('_id', pymongo.ASCENDING)
            if limit is not None:
                try:
                    parsed_limit = int(limit)
                except Exception:
                    parsed_limit = None
                if parsed_limit is not None and parsed_limit > 0:
                    cursor = cursor.limit(parsed_limit)

            messages = list(cursor)
            if delete and messages:
                ids = [message['_id'] for message in messages if message and '_id' in message]
                if ids:
                    collection.delete_many({'_id': {'$in': ids}})

            return messages if messages else None
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::consumer', 'error consuming message:{}'.format(e))

    def delete_invalid_activity_messages(self, topic, batch_size=200):
        try:
            collection = self._mq_collection()
            size = 200
            try:
                size = int(batch_size)
            except Exception:
                size = 200
            if size < 1:
                size = 1

            query = {
                'topic': topic,
                'activity': {'$type': 'binData'}
            }
            docs = list(collection.find(query, {'_id': 1}).sort('_id', pymongo.ASCENDING).limit(size))
            if not docs:
                return 0
            ids = [doc['_id'] for doc in docs if '_id' in doc]
            if not ids:
                return 0
            result = collection.delete_many({'_id': {'$in': ids}})
            return int(result.deleted_count)
        except Exception as e:
            logger.log_to_console(
                'ERROR', 'MQ::delete_invalid_activity_messages',
                'error deleting invalid activity messages:{}'.format(e)
            )
            return 0

    def delete_mq(self, message):
        try:
            self._mq_collection().delete_one({'_id':message['_id']})
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
            return True
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::delete_mq', 'error deleting message:{}, with error: {}'.format(message,e))


