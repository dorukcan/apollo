import importlib
import json

import attr
from sentry_sdk import capture_exception

from .database import Database
from .exceptions import SiteError
from .message import make_message


@attr.s(slots=True)
class Site:
    crawler_name = attr.ib()

    db = Database()
    schema_name = "apollo"
    table_name = None

    @property
    def module_path(self):
        return "apollo.crawlers" + "." + self.crawler_name

    @property
    def full_table_name(self):
        return self.schema_name + "." + self.table_name

    ########################################################

    def collect(self):
        # get data
        crawler = self.load_module()
        new_items = self.collect_site(crawler)

        # extract new data
        old_items = self.get_old_items()
        payload = self.extract_new_items(old_items, new_items)

        # save data
        self.save_data(payload)

    def log(self, msg=None, level="info", *args, **kwargs):
        make_message(
            self.crawler_name, msg, level,
            extra=json.dumps(dict(args=args, kwargs=kwargs), default=str)
        )

    ########################################################

    def load_module(self):
        try:
            module = importlib.import_module(self.module_path)
            crawler = module.Crawler()

            self.log('Module initialized.')
        except Exception as e:
            self.log('Can not initialized.', "error", e)
            capture_exception(e)
            raise SiteError

        return crawler

    def collect_site(self, crawler):
        try:
            items = crawler.collect()
            self.log('Items collected. {} items.'.format(len(items)))
        except Exception as e:
            self.log('Can not collect.', "error", e)
            capture_exception(e)
            raise SiteError

        return items

    ########################################################

    def get_old_items(self):
        raise NotImplementedError

    def extract_new_items(self, old_items, new_items):
        raise NotImplementedError

    def save_data(self, payload):
        payload = [{**item.as_dict(), **{
            "site_name": self.crawler_name
        }} for item in payload]

        try:
            self.db.insert_data(self.full_table_name, payload)
            self.log('Data saved. {} items.'.format(len(payload)))
        except Exception as e:
            self.log('Can not save.', "error", e)
            capture_exception(e)
            raise SiteError

        return payload

    def remove_duplicates(self):
        raise NotImplementedError

    ########################################################

    def run_query(self, query, callback=None, default=None):
        try:
            response = self.db.run_query(query)

            if callback is not None:
                return callback(response)
            else:
                return response
        except Exception as e:
            if not default:
                return default

            raise e


@attr.s(frozen=True)
class Entity:
    def as_dict(self):
        return attr.asdict(self)

    def as_json(self):
        return json.dumps(self.as_dict())

    def get(self, key):
        return getattr(self, key)


class Filter:
    @classmethod
    def with_keys(cls, old_items, new_items, name_fields, diff_fields):
        field_names = list(set(name_fields + diff_fields))

        ###################################################

        groups_by_name = {}

        for old_item in old_items:
            group_key = cls.make_key(old_item, field_names)
            groups_by_name[group_key] = old_item

        ###################################################

        result = []

        for new_item in new_items:
            group_key = cls.make_key(new_item, field_names)
            old_item = groups_by_name.get(group_key)

            if not old_item:
                result.append(new_item)
                groups_by_name[group_key] = new_item

        return result

    @classmethod
    def make_key(cls, item, field_names):
        field_values = [str(item.get(field_name)) for field_name in field_names]

        return json.dumps(field_values, default=str)
