import datetime
import itertools
import json
from urllib.parse import urlencode

from apollo.backend import BaseCrawler
from apollo.modules import Line, Record

headers = {
    "Accept": "application/json, text/plain, */*"
}


class Crawler(BaseCrawler):
    def collect(self):
        geos = ["US", "TR"]
        cats = ["b", "t", "h"]
        mapped_args = list(itertools.product(geos, cats))

        result = []

        for geo, cat in mapped_args:
            result.extend(self.real_time(geo=geo, cat=cat))

        return result

    ##################################################################

    def real_time(self, geo="TR", cat="h"):
        base_url = "https://trends.google.com.tr/trends/api/realtimetrends"
        url = base_url + "?" + urlencode({
            "hl": "en", "cat": cat, "geo": geo, "tz": -180,
            "fi": 0, "fs": 0, "ri": 300, "rs": 20, "sort": 0,
        })
        response = self.make_get(url=url, headers=headers)

        ###########################################################################

        geo_map = {
            "TR": "Türkiye",
            "US": "ABD",
        }
        cat_map = {
            "t": "Bilim/Teknoloji",
            "b": "İş",
            "h": "En çok görüntülenen Haberler",
        }

        raw_data = response.text.replace(')]}\'', '')
        data = json.loads(raw_data)

        stories = data["storySummaries"]["trendingStories"]
        id_list = [item["id"] for item in stories]

        lines_data = self.spark_lines(id_list)

        return [Record(
            id=item["id"],
            created_at=datetime.datetime.now(),
            category=cat_map.get(cat),
            country=geo_map.get(geo),

            entities=item['entityNames'],
            lines=lines_data.get(item["id"]),
            articles=[{
                "title": article["articleTitle"],
                "snippet": article["snippet"],
                "time": article["time"]
            } for article in item["articles"]],
        ) for item in stories]

    def spark_lines(self, id_list):
        base_url = "https://trends.google.com.tr/trends/api/widgetdata/sparkline"
        url = base_url + "?hl=en&tz=-180&id=" + "&id=".join(id_list)
        response = self.make_get(url=url, headers=headers)

        raw_data = response.text.replace(')]}\',', '')
        data = json.loads(raw_data)
        responses = data["default"]["response"]

        result = {}

        for id_, response in zip(id_list, responses):
            result[id_] = [Line(
                record_id=id_,
                time_at=item["time"],
                value=item["value"],
            ) for item in response["timelineResponse"]["timelineData"]]

        return result
