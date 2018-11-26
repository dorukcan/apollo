import json

import psycopg2.extras

from apollo.settings import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from .utils import merge_into_one

DEFAULT_DB = dict(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)


class SingletonMetaclass(type):
    def __call__(cls, *args, **kw):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SingletonMetaclass, cls).__call__(*args, **kw)
        return cls.instance


Singleton = SingletonMetaclass('Singleton', (object,), {})


class Database(Singleton):
    def __init__(self, **kwargs):
        db_params = DEFAULT_DB if not kwargs else kwargs

        self.conn = psycopg2.connect(**db_params,
                                     cursor_factory=psycopg2.extras.DictCursor)

    def run_query(self, query, **kwargs):
        if not query:
            return None

        response = None
        cur = self.conn.cursor()

        try:
            cur.execute(query, kwargs)

            # https://stackoverflow.com/a/35612749/1704515
            response = cur.fetchall()
        except Exception as e:
            if str(e) not in ['no results to fetch']:
                raise e
        finally:
            self.conn.commit()
            cur.close()

        return response

    def run_query_many(self, query, data):
        payload = []

        for item in data:
            row = []

            for col in item.values():
                if type(col) in [set, list, dict]:
                    col = json.dumps(col, default=str)

                row.append(col)

            payload.append(tuple(row))

        cur = self.conn.cursor()
        cur.executemany(query, tuple(payload))

        self.conn.commit()
        cur.close()

    ###############################################################

    def insert_data(self, table_name, data, create_table=True):
        if not data:
            return

        data = [data] if type(data) not in [set, list] else data

        if create_table:
            self.create_table(table_name, data)

        sample_item = merge_into_one(data)
        insert_query = self.dict_to_insert_query(table_name, sample_item)

        self.run_query_many(insert_query, data)

    def create_table(self, table_name, data, primary_key=None):
        self.create_schema(table_name.split('.')[0])

        sample_item = merge_into_one(data)
        create_query = self.dict_to_create_query(table_name, sample_item, primary_key)

        self.run_query(create_query)

    def create_schema(self, schema_name):
        query = "CREATE SCHEMA IF NOT EXISTS {};".format(schema_name)

        self.run_query(query)

    ###############################################################

    def dict_to_insert_query(self, table_name, sample_item):
        fields_stmt = ", ".join([field for field in sample_item.keys()])
        values_stmt = ','.join(['%s'] * len(list(sample_item.keys())))

        return "INSERT INTO {table_name} ({fields_stmt}) VALUES ({values_stmt})".format(
            table_name=table_name, fields_stmt=fields_stmt, values_stmt=values_stmt
        )

    def dict_to_create_query(self, table_name, sample_item, primary_key=None):
        columns = ["{col_name} {col_type} {constraints}".format(
            col_name=key,
            col_type=self.make_sql_type(value),
            constraints="PRIMARY KEY" if primary_key else "NULL"
        ) for key, value in sample_item.items()]

        columns_stmt = ", ".join(column for column in columns)

        return "CREATE TABLE IF NOT EXISTS {table_name} ({columns_stmt});".format(
            table_name=table_name, columns_stmt=columns_stmt
        )

    ###############################################################

    def make_sql_type(self, value):
        field_map = {
            "<class 'int'>": "NUMERIC",
            "<class 'float'>": "NUMERIC",
            "<class 'datetime.datetime'>": "TIMESTAMP",
            "<class 'datetime.date'>": "DATE",
            "<class 'datetime.timedelta'>": "TEXT",
            "<class 'dict'>": "JSONB",
            "<class 'list'>": "JSONB",
            "<class 'set'>": "JSONB",
            "<class 'bool'>": "BOOLEAN",
            "<class 'str'>": "TEXT",
            "<class 'bytes'>": "BYTEA",
            "<class 'NoneType'>": "TEXT",
        }

        return field_map.get(str(type(value)), 'BYTEA')


# TODO
class Backup:
    pass
