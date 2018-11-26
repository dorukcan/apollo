import datetime
import re
from urllib.parse import urlencode

from apollo.backend import BaseCrawler, build_soup, q, qs, qst, qsu
from apollo.modules import Product


def make_price(txt):
    new_txt = txt.replace('.', '').replace(',', '.').replace(' TL', '')

    if new_txt:
        return float(new_txt)
    else:
        return 0


class Crawler(BaseCrawler):
    def __init__(self):
        super().__init__()
        self.base_url = 'https://www.sahibinden.com'

    def collect(self):
        result = self.get_category_full("laptop-bilgisayar")
        products = self.make_products(result)

        return products

    def make_products(self, raw_items):
        return [Product(
            code=item["url"],
            url=item["url"],
            title=item["title"],
            brand=item["store"],
            category=item["category"],
            price=item["price"],
            created_at=datetime.datetime.now(),
        ) for item in raw_items]

    ############################################################

    def get_category_full(self, category_key):
        _, max_page = self.get_category(category_key)

        page_size = 50

        result = []

        for offset in range(0, max_page * page_size, page_size):
            data, _ = self.get_category(category_key, offset)
            result.extend(data)

        return result

    def get_category(self, category_key, offset=0):
        page_size = 50

        # generate data
        params = urlencode({
            "pagingOffset": offset,
            "pagingSize": page_size,
            "price_min": 2000,
            "price_max": 4000,
            "sorting": "date_desc",
            "address_region": "1",
            "address_city": "34",
            "address_town": "418",
        })
        url = self.base_url + '/' + category_key + '?' + params
        response = self.make_get(url)

        return self.extract_category(category_key, response)

    def extract_category(self, category_key, response):
        soup = build_soup(response.text)

        # extract products
        result = [{
            "id": item.get('data-id'),
            "category": category_key,
            "title": qst(item, '.classifiedTitle', True),
            "url": qsu(item, ".classifiedTitle"),
            "store": qs(item, '.store-icon').get('title'),
            "store_url": qsu(item, '.store-icon'),
            "price": make_price(qst(item, '.searchResultsPriceValue', True)),
            "published_at": qst(item, '.searchResultsDateValue', True),
            "location": qst(item, '.searchResultsLocationValue', True),
        } for item in q(soup, '.searchResultsRowClass .searchResultsItem') if item.get('data-id')]

        # detect max page
        max_page_text = qst(soup, '.pageNavigator .mbdef', True)
        max_page = re.search('Toplam (.*?) sayfa içerisinde.*?sayfayı görmektesiniz', max_page_text)

        if max_page:
            max_page = int(max_page.groups()[0])

        return result, max_page

    ############################################################

    def get_category_list(self):
        response = self.make_get(self.base_url)
        soup = build_soup(response.text)

        return [{
            "title": item.text.strip(),
            "url": item.get('href', '').replace('kategori/', ''),
        } for item in q(soup, '.categories-left-menu a')]

    ############################################################

    def get_city_list(self):
        # https://www.sahibinden.com/ajax/search/locationFacets?vcIncluded=true&pagingSize=50&language=tr&price_min=2000&category=111812&price_max=4000&address_country=1
        pass

    def get_town_list(self, city_id):
        # https://www.sahibinden.com/ajax/location/loadTownsByCityIds?address_city=34
        pass
