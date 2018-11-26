import attr

from apollo.backend import Entity, Filter, Site


@attr.s
class Social(Site):
    # db, name
    table_name = 'social'

    def get_old_items(self):
        query = """
            SELECT DISTINCT ON(code) code, source, metric_name, metric_value, type_key, created_at
            FROM {table_name}
            WHERE site_name = '{site_name}'
            ORDER BY code, created_at DESC
        """.format(table_name=self.full_table_name, site_name=self.crawler_name)

        return self.run_query(query, default=[])

    def extract_new_items(self, old_items, new_items):
        name_fields = ["code", "source", "metric_name", "type_key"]
        diff_fields = ["metric_value"]

        return Filter.with_keys(old_items, new_items, name_fields, diff_fields)

    def remove_duplicates(self):
        return

        query = """
            SELECT code,
                   source,
                   site_name,
                   metric_name,
                   COUNT(*)                                                                            AS count_val,
                   json_agg(json_build_object('metric_value', metric_value, 'created_at', created_at)) AS records
            FROM {table_name}
            WHERE site_name = '{site_name}'
            GROUP BY code, source, site_name, metric_name
            ORDER BY code, source, site_name, metric_name;
        """.format(table_name=self.full_table_name, site_name=self.crawler_name)

        response = self.run_query(query)
        rows_to_delete = []

        for row in response:
            unique_records = []

            for record in row["records"]:
                pass


@attr.s
class Trackable(Entity):
    code = attr.ib()
    parent_code = attr.ib()

    source = attr.ib()
    type_key = attr.ib()

    metric_name = attr.ib()
    metric_value = attr.ib()

    created_at = attr.ib()
