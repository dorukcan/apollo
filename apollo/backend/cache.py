import datetime
import json
import pickle

from .database import Database


def is_expired(created_at, expire_value):
    expire_days = int(expire_value)
    expire_hours = int((expire_value - expire_days) * 24)

    elapsed = datetime.timedelta(days=expire_days, hours=expire_hours)

    return datetime.datetime.now() > created_at + elapsed


class Cache:
    def __init__(self):
        self.db = Database()

        self.schema_name = 'apollo'
        self.table_name = 'cache'

        self._create_table()

    @property
    def full_name(self):
        return self.schema_name + "." + self.table_name

    def get(self, key):
        response = self.db.run_query("""
            SELECT cache_content, created_at, expire_days
            FROM {full_name}
            WHERE cache_key = '{key}'
            LIMIT 1
        """.format(full_name=self.full_name, key=key))

        if not response:
            return None

        return self._deserialize_data(response[0]['cache_content'])

    def put(self, key, value, expire_days=30):
        value = self._serialize_data(value)
        payload = {
            "cache_key": key,
            "cache_content": value,
            "created_at": datetime.datetime.now(),
            "expire_days": expire_days
        }

        try:
            self.db.insert_data(self.full_name, payload)
        except:
            print('cache error: cannot put')

    def is_cached(self, key):
        return self.get(key) is not None

    def delete(self, key):
        self.db.run_query("DELETE FROM {full_name} WHERE url = %(key)s".format(
            full_name=self.full_name
        ), key=key)

    ############################################################

    def _create_table(self):
        self._create_schema()

        query = """
            CREATE TABLE IF NOT EXISTS {full_name} (
                cache_key         text not null primary key,
                cache_content     bytea,
                created_at        timestamp,
                expire_days       numeric
            );
        """.format(full_name=self.full_name)

        self.db.run_query(query)

    def _create_schema(self):
        query = "CREATE SCHEMA IF NOT EXISTS {};".format(self.schema_name)

        self.db.run_query(query)

    def _serialize_data(self, data):
        return pickle.dumps(data)

    def _deserialize_data(self, content):
        return pickle.loads(content)

    ############################################################

    def get_batch(self, key_list):
        results = self.db.run_query("""
            SELECT cache_key, cache_content, created_at, expire_days
            FROM {full_name}
            WHERE url IN %(key_list)s
        """.format(full_name=self.full_name), key_list=tuple(key_list))

        for res in results:
            yield self._deserialize_data(res["cache_content"])

    def is_cached_multi(self, cache_keys):
        response = self.db.run_query("""
            SELECT cache_key
            FROM {full_name}
            WHERE cache_key IN %(keys)s
        """.format(full_name=self.full_name), keys=tuple(cache_keys))

        cached_keys = [x["cache_key"] for x in response]
        return {cache_key: cache_key in cached_keys for cache_key in cache_keys}


def cached_function(fn):
    def wrapper(*args, **kwargs):
        _args = args

        if len(_args) > 0 and str(type(_args[0])).startswith("<class"):
            _args = _args[1:]

        cache_key = "{fn_name}-{args}-{kwargs}".format(
            fn_name=fn.__name__,
            args=json.dumps(_args, default=str),
            kwargs=json.dumps(kwargs, default=str)
        )

        ####################

        cache_obj = Cache()
        result = cache_obj.get(cache_key)

        if not result:
            result = fn(*args, **kwargs)
            cache_obj.put(cache_key, result)
        else:
            result = json.loads(result)

        return result

    return wrapper
