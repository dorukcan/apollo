import attr

from apollo.backend import Entity, Filter, Site


@attr.s
class Trend(Site):
    table_name = 'trend'

    def get_old_items(self):
        query = """
            SELECT DISTINCT id, country, category, lines, created_at
            FROM {full_table_name}
            WHERE site_name = '{site_name}'
            ORDER BY id, created_at DESC;
        """.format(full_table_name=self.full_table_name, site_name=self.crawler_name)

        return self.run_query(query, default=[])

    def extract_new_items(self, old_items, new_items):
        name_fields = ["id"]
        diff_fields = ["lines"]

        return Filter.with_keys(old_items, new_items, name_fields, diff_fields)

    def remove_duplicates(self):
        pass


@attr.s
class Record(Entity):
    id = attr.ib()
    country = attr.ib()
    category = attr.ib()

    created_at = attr.ib()
    entities = attr.ib()
    lines = attr.ib()
    articles = attr.ib()


@attr.s
class Line(Entity):
    record_id = attr.ib()
    time_at = attr.ib()
    value = attr.ib()
