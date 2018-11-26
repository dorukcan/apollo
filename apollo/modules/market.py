import attr

from apollo.backend import Entity, Filter, Site


@attr.s
class Market(Site):
    table_name = 'market'

    def get_old_items(self):
        query = """
            SELECT DISTINCT code, price, created_at
            FROM {table_name}
            WHERE site_name = '{site_name}'
            ORDER BY code, created_at DESC;
        """.format(table_name=self.full_table_name, site_name=self.crawler_name)

        return self.run_query(query, default=[])

    def extract_new_items(self, old_items, new_items):
        name_fields = ["code"]
        diff_fields = ["price"]

        return Filter.with_keys(old_items, new_items, name_fields, diff_fields)

    def remove_duplicates(self):
        query = """
            WITH _products AS (SELECT site_name, code, price, MAX(created_at) AS last_date, COUNT(*) AS count_val
                               FROM {table_name}
                               WHERE site_name = {site_name}
                               GROUP BY site_name, code, price
                               ORDER BY COUNT(*) DESC, site_name, code, price),
                 _duplicates AS (SELECT t1.site_name, t1.code, t1.price, t2.last_date
                                 FROM {table_name} t1
                                        INNER JOIN _products t2 ON t2.site_name = t1.site_name
                                                                     AND t2.code = t1.code
                                                                     AND t2.price = t1.price
                                 WHERE count_val > 1)

            DELETE FROM {table_name}
            WHERE (site_name, code, price) IN (SELECT site_name, code, price FROM _duplicates)
              AND created_at NOT IN (SELECT last_date FROM _duplicates);
        """.format(table_name=self.full_table_name, site_name=self.crawler_name)

        # self.run_query(query)


@attr.s
class Product(Entity):
    code = attr.ib(default=None)
    url = attr.ib(default=None)

    title = attr.ib(default=None)
    brand = attr.ib(default=None)
    category = attr.ib(default=None)

    price = attr.ib(default=None)
    created_at = attr.ib(default=None)
