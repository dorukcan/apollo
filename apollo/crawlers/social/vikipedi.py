import datetime
import json

from apollo.backend import BaseCrawler, Database
from apollo.modules import Trackable


class Crawler(BaseCrawler):
    def collect(self):
        result = []

        titles = self.prepare_title_names()

        for i in range(0, len(titles), 50):
            result.extend(self.page_views(titles[i:i + 50]))

        return result

    def prepare_title_names(self):
        response = Database().run_query("""
            SELECT jsonb_array_elements_text(entities) AS keyword, 
                   COUNT(*) AS count_val
            FROM apollo.trend
            GROUP BY keyword
            HAVING COUNT(*) > 3
            ORDER BY count_val DESC
            LIMIT 100;
        """)

        result = [item["keyword"] for item in response]

        if not result:
            result = ['Steve Jobs', "Recep Tayyip Erdoğan", "Abdullah Gül"]

        return result

    def page_views(self, titles):
        metric_name = "page_views"
        source = "wikipedia"

        url = "https://tr.wikipedia.org/w/api.php"
        url += "?action=query"
        url += "&format=json"
        url += "&formatversion=2"
        url += "&prop=pageviews"
        url += "&titles=" + "|".join(titles)

        response = self.make_get(url)
        pages = json.loads(response.text)["query"]["pages"]

        result = []

        for page in pages:
            if "missing" not in page and "pageviews" in page:
                views_obj = page["pageviews"]

                for key, value in views_obj.items():
                    result.append(Trackable(
                        code=page["title"],
                        parent_code=None,
                        source=source,
                        metric_name=metric_name,
                        metric_value=value,
                        type_key=key,
                        created_at=datetime.datetime.now()
                    ))

        return result


if __name__ == "__main__":
    Crawler().collect()
