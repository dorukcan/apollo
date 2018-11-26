import datetime

from apollo.backend import BaseCrawler, build_soup, q, qs, qst
from apollo.modules import Trackable


class Crawler(BaseCrawler):
    def __init__(self):
        super().__init__()

    def collect(self):
        items = self.get_collection("recommended_for_you_POPULAR_APPS_GAMES")
        result = []

        for doc_id in items:
            data = self.app_details(doc_id)
            result.append(data)

        return self.make_records(result)

    def get_collection(self, collection_key):
        url = "https://play.google.com/store/apps/collection/" + collection_key + "?clp=ogoGCAQqAggB:S:ANO1ljIlc5c"
        response = self.make_get(url)
        soup = build_soup(response.text)

        return [item.get('data-docid') for item in q(soup, '.id-card-list .card')]

    def app_details(self, app_id):
        response = self.make_get(url="https://play.google.com/store/apps/details?id=" + app_id)
        soup = build_soup(response.text)

        return {
            "app_id": app_id,
            "image": qs(soup, "[itemprop='image']").get('src'),
            "name": qst(soup, "[itemprop='name']", True),
            "genre": qst(soup, "[itemprop='genre']", True),
            "description": qs(soup, "[itemprop='description']").get('content'),
            "price": float(qs(soup, "[itemprop='price']").get('content', '0')),
            "rating_value": float(qs(soup, "[itemprop='ratingValue']").get('content')),
            "review_count": float(qs(soup, "[itemprop='reviewCount']").get('content')),
        }

    def make_records(self, apps):
        source = "play_store"
        result = []

        app_metrics = [
            # "price",
            "rating_value",
            "review_count"
        ]

        for app_ in apps:
            result.extend(self.make_record(app_, "app_id", None, source, app_metrics, "app"))

        return result

    def make_record(self, item, code, parent_code, source, metrics, type_key):
        return [Trackable(
            code=item[code],
            parent_code=item[parent_code] if parent_code else None,
            source=source,
            metric_name=metric_name,
            metric_value=item[metric_name],
            type_key=type_key,
            created_at=datetime.datetime.now()
        ) for metric_name in metrics]
