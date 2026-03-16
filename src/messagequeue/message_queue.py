from dataclasses import dataclass
import pymongo
import os
from datetime import datetime
from smartezlogger import logger

@dataclass
class MQ():
    def __init__(self):
        self.mongo_db = None
        self._mongo_error = None
        self._canonical_db_name = os.getenv('MQ_CANONICAL_DB', 'smartez')
        self._canonical_collection_name = os.getenv('MQ_CANONICAL_COLLECTION', 'MQ')
        self._shard_db_prefix = os.getenv('MQ_SHARD_DB_PREFIX', 'smartez_mq_')
        self._shard_collection_name = os.getenv('MQ_SHARD_COLLECTION', 'MQ')
        self._secretary_checkpoint_collection = os.getenv('MQ_SECRETARY_CHECKPOINT_COLLECTION', 'MQSecretaryCheckpoint')
        self._secretary_checkpoint_key = os.getenv('MQ_SECRETARY_CHECKPOINT_KEY', 'fanout')
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
        return self.mongo_db[self._canonical_db_name][self._canonical_collection_name]

    def _checkpoint_collection(self):
        if self.mongo_db is None:
            if self._mongo_error is not None:
                raise RuntimeError(f'MongoDB client is not initialized: {self._mongo_error}')
            raise RuntimeError('MongoDB client is not initialized')
        return self.mongo_db[self._canonical_db_name][self._secretary_checkpoint_collection]

    def _sanitize_topic(self, topic):
        value = str(topic or '').strip().lower()
        if value == '':
            value = 'default'
        chars = []
        for char in value:
            if char.isalnum() or char in ['_', '-']:
                chars.append(char)
            else:
                chars.append('_')
        return ''.join(chars)

    def _topic_collection(self, topic):
        topic_safe = self._sanitize_topic(topic)
        db_name = f"{self._shard_db_prefix}{topic_safe}"
        return self.mongo_db[db_name][self._shard_collection_name]

    def _ensure_collection_indexes(self, collection):
        collection.create_index([('topic', 1), ('_id', 1)])
        collection.create_index([('topic', 1), ('activity', 1), ('_id', 1)])
        collection.create_index([('activity', 1), ('user_id', 1)])

    def _ensure_indexes(self):
        try:
            collection = self._mq_collection()
            self._ensure_collection_indexes(collection)
            self._checkpoint_collection().create_index([('_id', 1)], unique=True)
        except Exception as e:
            logger.log_to_console('WARNING', 'MQ::_ensure_indexes', 'failed to ensure MQ indexes: {}'.format(e))

    def _query_with_activity(self, topic, activity=None):
        query = {'topic': topic}
        if activity is not None:
            query['activity'] = activity
        return query

    def _find_one_with_fallback(self, topic, query):
        shard_collection = self._topic_collection(topic)
        try:
            self._ensure_collection_indexes(shard_collection)
        except Exception:
            pass

        message = shard_collection.find_one(query, sort=[('_id', pymongo.ASCENDING)])
        if message is not None:
            return message
        return self._mq_collection().find_one(query, sort=[('_id', pymongo.ASCENDING)])

    def _find_many_with_fallback(self, topic, query, limit=None):
        shard_collection = self._topic_collection(topic)
        try:
            self._ensure_collection_indexes(shard_collection)
        except Exception:
            pass

        cursor = shard_collection.find(query).sort('_id', pymongo.ASCENDING)
        parsed_limit = None
        if limit is not None:
            try:
                parsed_limit = int(limit)
            except Exception:
                parsed_limit = None
            if parsed_limit is not None and parsed_limit > 0:
                cursor = cursor.limit(parsed_limit)
        messages = list(cursor)
        if messages:
            return messages

        canonical_cursor = self._mq_collection().find(query).sort('_id', pymongo.ASCENDING)
        if parsed_limit is not None and parsed_limit > 0:
            canonical_cursor = canonical_cursor.limit(parsed_limit)
        return list(canonical_cursor)

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

    def secretary_fanout_once(self, batch_size=500, topic=None):
        try:
            size = 500
            try:
                size = int(batch_size)
            except Exception:
                size = 500
            if size < 1:
                size = 1

            checkpoint_collection = self._checkpoint_collection()
            checkpoint = checkpoint_collection.find_one({'_id': self._secretary_checkpoint_key}) or {}
            last_id = checkpoint.get('last_id')

            query = {}
            if topic is not None:
                query['topic'] = topic
            if last_id is not None:
                query['_id'] = {'$gt': last_id}

            messages = list(
                self._mq_collection()
                .find(query)
                .sort('_id', pymongo.ASCENDING)
                .limit(size)
            )

            if not messages:
                return {
                    'processed': 0,
                    'last_id': str(last_id) if last_id is not None else None
                }

            processed = 0
            for message in messages:
                topic_value = message.get('topic', 'default')
                shard_collection = self._topic_collection(topic_value)
                try:
                    self._ensure_collection_indexes(shard_collection)
                except Exception:
                    pass
                shard_collection.replace_one({'_id': message['_id']}, message, upsert=True)
                processed += 1

            new_last_id = messages[-1]['_id']
            checkpoint_collection.update_one(
                {'_id': self._secretary_checkpoint_key},
                {
                    '$set': {
                        'last_id': new_last_id,
                        'updated_at': datetime.utcnow()
                    }
                },
                upsert=True
            )

            return {
                'processed': processed,
                'last_id': str(new_last_id)
            }
        except Exception as e:
            logger.log_to_console('ERROR', 'MQ::secretary_fanout_once', 'error during fanout: {}'.format(e))
            return {
                'processed': 0,
                'last_id': None,
                'error': str(e)
            }

    def consumer(self, topic, delete=False, all=False, activity=None, limit=None):
        ''' Interface for the consumer to get messages from the queue
         '''
        try:
            query = self._query_with_activity(topic, activity)
            if not all:
                message = self._find_one_with_fallback(topic, query)
                if message and delete:
                    self.delete_mq(message)
                return message

            messages = self._find_many_with_fallback(topic, query, limit=limit)
            if delete and messages:
                ids = [message['_id'] for message in messages if message and '_id' in message]
                if ids:
                    self._mq_collection().delete_many({'_id': {'$in': ids}})
                    try:
                        self._topic_collection(topic).delete_many({'_id': {'$in': ids}})
                    except Exception:
                        pass

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
            try:
                self._topic_collection(topic).delete_many({'_id': {'$in': ids}})
            except Exception:
                pass
            return int(result.deleted_count)
        except Exception as e:
            logger.log_to_console(
                'ERROR', 'MQ::delete_invalid_activity_messages',
                'error deleting invalid activity messages:{}'.format(e)
            )
            return 0

    def delete_mq(self, message):
        try:
            message_id = message.get('_id')
            message_topic = message.get('topic')
            if message_id is None:
                return False

            self._mq_collection().delete_one({'_id': message_id})
            if message_topic is not None:
                try:
                    self._topic_collection(message_topic).delete_one({'_id': message_id})
                except Exception:
                    pass
            logger.log_to_console(
                    'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
            return True
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::delete_mq', 'error deleting message:{}, with error: {}'.format(message,e))


