from dataclasses import dataclass
import pymongo
import os
import time
from datetime import datetime
from pymongo.errors import AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError
from smartezlogger import logger

@dataclass
class MQ():
    def __init__(self):
        self.mongo_db = None
        self._mongo_error = None
        self._mongo_url = None
        self._canonical_db_name = os.getenv('MQ_CANONICAL_DB', 'smartez')
        self._canonical_collection_name = os.getenv('MQ_CANONICAL_COLLECTION', 'MQ')
        self._shard_db_prefix = os.getenv('MQ_SHARD_DB_PREFIX', 'smartez_mq_')
        self._shard_collection_name = os.getenv('MQ_SHARD_COLLECTION', 'MQ')
        self._topic_override_db_name = os.getenv('MQ_TOPIC_OVERRIDE_DB', 'smartez_mq')
        self._topic_override_collection_prefix = os.getenv('MQ_TOPIC_OVERRIDE_COLLECTION_PREFIX', 'MQ_')
        raw_topic_overrides = os.getenv('MQ_TOPIC_OVERRIDES', 'categorize')
        self._topic_overrides = {
            str(topic).strip().lower()
            for topic in str(raw_topic_overrides).split(',')
            if str(topic).strip()
        }
        raw_topic_route_map = os.getenv('MQ_TOPIC_ROUTE_MAP', 'categorize=smartez_mq.MQ_categorize')
        self._topic_route_map = self._parse_topic_route_map(raw_topic_route_map)
        self._route_logs = set()
        self._secretary_checkpoint_collection = os.getenv('MQ_SECRETARY_CHECKPOINT_COLLECTION', 'MQSecretaryCheckpoint')
        self._secretary_checkpoint_key = os.getenv('MQ_SECRETARY_CHECKPOINT_KEY', 'fanout')
        try:
            self._mongo_url = os.getenv('MONGODB_URL') or os.getenv('MONGO_URI')
            if not self._mongo_url:
                raise RuntimeError('Missing MongoDB connection string. Set MONGODB_URL or MONGO_URI')
            self._connect_client()
        except Exception as e:
            self._mongo_error = e
            logger.log_to_console('ERROR', 'MQ::__init__', f'failed to initialize MongoDB client: {e}')

    def _mongo_retry_attempts(self):
        try:
            attempts = int(os.getenv('MQ_MONGO_RETRY_ATTEMPTS', '3'))
        except Exception:
            attempts = 3
        return max(1, attempts)

    def _mongo_retry_sleep_seconds(self):
        try:
            delay = float(os.getenv('MQ_MONGO_RETRY_DELAY_SECONDS', '1.0'))
        except Exception:
            delay = 1.0
        return max(0.0, delay)

    def _is_retryable_mongo_error(self, error):
        return isinstance(error, (AutoReconnect, ConnectionFailure, NetworkTimeout, ServerSelectionTimeoutError))

    def _build_client(self):
        return pymongo.MongoClient(self._mongo_url)

    def _connect_client(self):
        if not self._mongo_url:
            raise RuntimeError('Missing MongoDB connection string. Set MONGODB_URL or MONGO_URI')
        client = self._build_client()
        self.mongo_db = client
        self._mongo_error = None
        self._ensure_indexes()
        return client

    def reconnect(self):
        old_client = self.mongo_db
        self.mongo_db = None
        try:
            if old_client is not None:
                old_client.close()
        except Exception:
            pass
        return self._connect_client()

    def execute_with_retry(self, operation_name, operation):
        last_error = None
        attempts = self._mongo_retry_attempts()
        delay = self._mongo_retry_sleep_seconds()
        for attempt in range(1, attempts + 1):
            try:
                if self.mongo_db is None:
                    self.reconnect()
                return operation()
            except Exception as error:
                last_error = error
                self._mongo_error = error
                if not self._is_retryable_mongo_error(error) or attempt >= attempts:
                    raise
                logger.log_to_console(
                    'WARNING',
                    'MQ::execute_with_retry',
                    'operation={} attempt={}/{} failed: {}'.format(operation_name, attempt, attempts, error)
                )
                try:
                    self.reconnect()
                except Exception as reconnect_error:
                    self._mongo_error = reconnect_error
                    logger.log_to_console(
                        'WARNING',
                        'MQ::execute_with_retry',
                        'operation={} reconnect attempt {}/{} failed: {}'.format(operation_name, attempt, attempts, reconnect_error)
                    )
                if delay > 0:
                    time.sleep(delay * attempt)
        if last_error is not None:
            raise last_error
        return None

    def _parse_topic_route_map(self, raw_value):
        route_map = {}
        if raw_value is None:
            return route_map

        for part in str(raw_value).split(','):
            entry = str(part).strip()
            if not entry or '=' not in entry:
                continue
            topic_key, collection_target = entry.split('=', 1)
            topic_value = self._sanitize_topic(topic_key)
            target = str(collection_target).strip()
            if not topic_value or '.' not in target:
                continue
            db_name, collection_name = target.split('.', 1)
            db_name = str(db_name).strip()
            collection_name = str(collection_name).strip()
            if db_name and collection_name:
                route_map[topic_value] = (db_name, collection_name)
        return route_map

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

    def _topic_override_collection(self, topic):
        topic_safe = self._sanitize_topic(topic)
        mapped_target = self._topic_route_map.get(topic_safe)
        if mapped_target is not None:
            db_name, collection_name = mapped_target
            return self.mongo_db[db_name][collection_name]
        if topic_safe not in self._topic_overrides:
            return None
        collection_name = f"{self._topic_override_collection_prefix}{topic_safe}"
        return self.mongo_db[self._topic_override_db_name][collection_name]

    def _is_strict_routed_topic(self, topic):
        topic_safe = self._sanitize_topic(topic)
        return topic_safe in self._topic_route_map

    def _route_message(self, topic, collection):
        try:
            route_key = f"{topic}:{collection.full_name}"
            if route_key not in self._route_logs:
                self._route_logs.add(route_key)
                logger.log_to_console(
                    'INFO',
                    'MQ::route',
                    'topic={} collection={}'.format(topic, collection.full_name)
                )
        except Exception:
            pass

    def _ensure_collection_indexes(self, collection):
        collection.create_index([('topic', 1), ('_id', 1)])
        collection.create_index([('topic', 1), ('activity', 1), ('_id', 1)])
        collection.create_index([('activity', 1), ('user_id', 1)])

    def _ensure_indexes(self):
        try:
            collection = self._mq_collection()
            self._ensure_collection_indexes(collection)
            route_topics = set(self._topic_overrides)
            for topic in self._topic_route_map.keys():
                route_topics.add(topic)
            for topic in route_topics:
                override_collection = self._topic_override_collection(topic)
                if override_collection is not None:
                    self._ensure_collection_indexes(override_collection)
            self._checkpoint_collection().create_index([('_id', 1)], unique=True)
        except Exception as e:
            logger.log_to_console('WARNING', 'MQ::_ensure_indexes', 'failed to ensure MQ indexes: {}'.format(e))

    def _query_with_activity(self, topic, activity=None):
        query = {'topic': topic}
        if activity is not None:
            query['activity'] = activity
        return query

    def _find_one_with_fallback(self, topic, query):
        override_collection = self._topic_override_collection(topic)
        if override_collection is not None:
            try:
                self._ensure_collection_indexes(override_collection)
            except Exception:
                pass
            self._route_message(topic, override_collection)
            message = override_collection.find_one(query, sort=[('_id', pymongo.ASCENDING)])
            if message is not None:
                return message
            if self._is_strict_routed_topic(topic):
                return None

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
        override_collection = self._topic_override_collection(topic)
        if override_collection is not None:
            try:
                self._ensure_collection_indexes(override_collection)
            except Exception:
                pass

        shard_collection = self._topic_collection(topic)
        try:
            self._ensure_collection_indexes(shard_collection)
        except Exception:
            pass

        parsed_limit = None
        if limit is not None:
            try:
                parsed_limit = int(limit)
            except Exception:
                parsed_limit = None

        if override_collection is not None:
            self._route_message(topic, override_collection)
            override_cursor = override_collection.find(query).sort('_id', pymongo.ASCENDING)
            if parsed_limit is not None and parsed_limit > 0:
                override_cursor = override_cursor.limit(parsed_limit)
            override_messages = list(override_cursor)
            if override_messages:
                return override_messages
            if self._is_strict_routed_topic(topic):
                return []

        cursor = shard_collection.find(query).sort('_id', pymongo.ASCENDING)
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
        def operation():
            message: dict = {}
            message = {
                'topic': topic,
                'activity': activity,
                'user_id': user_id,
                'params': params
            }
            write_collection = self._topic_override_collection(topic)
            if write_collection is None:
                write_collection = self._mq_collection()
            else:
                self._route_message(topic, write_collection)

            if unique:
                result = write_collection.find_one({'activity':activity, 'user_id':user_id, 'params':params} )
                if result:
                    return True
                if write_collection != self._mq_collection() and not self._is_strict_routed_topic(topic):
                    result = self._mq_collection().find_one({'activity':activity, 'user_id':user_id, 'params':params} )
                if result:
                    return True
            write_collection.insert_one(message)
            return message

        return self.execute_with_retry('producer', operation)

    def secretary_fanout_once(self, batch_size=500, topic=None):
        try:
            def operation():
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

            return self.execute_with_retry('secretary_fanout_once', operation)
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
            def operation():
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
                        override_collection = self._topic_override_collection(topic)
                        if override_collection is not None:
                            self._route_message(topic, override_collection)
                            override_collection.delete_many({'_id': {'$in': ids}})
                        if not self._is_strict_routed_topic(topic):
                            self._mq_collection().delete_many({'_id': {'$in': ids}})
                            try:
                                self._topic_collection(topic).delete_many({'_id': {'$in': ids}})
                            except Exception:
                                pass

                return messages if messages else None

            return self.execute_with_retry('consumer', operation)
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::consumer', 'error consuming message:{}'.format(e))

    def delete_invalid_activity_messages(self, topic, batch_size=200):
        try:
            def operation():
                collection = self._topic_override_collection(topic)
                if collection is None:
                    collection = self._mq_collection()
                else:
                    self._route_message(topic, collection)
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
                if not self._is_strict_routed_topic(topic):
                    try:
                        self._topic_collection(topic).delete_many({'_id': {'$in': ids}})
                    except Exception:
                        pass
                return int(result.deleted_count)

            return self.execute_with_retry('delete_invalid_activity_messages', operation)
        except Exception as e:
            logger.log_to_console(
                'ERROR', 'MQ::delete_invalid_activity_messages',
                'error deleting invalid activity messages:{}'.format(e)
            )
            return 0

    def delete_mq(self, message):
        try:
            def operation():
                message_id = message.get('_id')
                message_topic = message.get('topic')
                if message_id is None:
                    return False

                strict_topic = False
                if message_topic is not None:
                    strict_topic = self._is_strict_routed_topic(message_topic)

                if not strict_topic:
                    self._mq_collection().delete_one({'_id': message_id})
                if message_topic is not None:
                    override_collection = self._topic_override_collection(message_topic)
                    if override_collection is not None:
                        self._route_message(message_topic, override_collection)
                        override_collection.delete_one({'_id': message_id})
                if message_topic is not None and not strict_topic:
                    try:
                        self._topic_collection(message_topic).delete_one({'_id': message_id})
                    except Exception:
                        pass
                logger.log_to_console(
                        'INFO', 'MQ::delete_mq', 'handled message succesfully:{}'.format(message))
                return True

            return self.execute_with_retry('delete_mq', operation)
        except Exception as e:
            logger.log_to_console(
                    'ERROR', 'MQ::delete_mq', 'error deleting message:{}, with error: {}'.format(message,e))


