import datetime
import logging

import requests

from apollo.backend import BaseCrawler, build_soup, q, qst, qsu
from apollo.modules import Product

logging.basicConfig(level=logging.DEBUG)
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.HTTPWarning)


def make_price(content):
    return float(content.replace(' TL', '').replace('.', '').replace(',', '.').strip())


class Crawler(BaseCrawler):
    def collect(self):
        category_list = self.get_category_list()

        result = []

        for category in category_list:
            result.extend(self.get_category(category))

        return result

    def get_category_list(self):
        response = self.make_get_req(url="https://online.a101.com.tr/", verify=False)
        soup = build_soup(response.text)

        return [{
            "name": item.text_content().strip(),
            "slug": item.get('href', '').replace('/', '')
        } for item in q(soup, '.nav-main .sub-nav a')]

    def get_category(self, category):
        response = self.make_get_req(url="https://online.a101.com.tr/" + category["slug"] + "/?ps=1000", verify=False)
        soup = build_soup(response.text)

        result = []

        for product in q(soup, '.ems-prd'):
            result.append(Product(
                code=qsu(product, '.ems-prd-code a'),
                url="https://online.a101.com.tr" + qsu(product, '.ems-prd-code a'),
                title=qst(product, '.ems-prd-name', True),
                brand=qst(product, '.ems-prd-marka', True),
                price=make_price(qst(product, '.ems-prd-price-selling')),
                category=category["name"],
                created_at=datetime.datetime.now()
            ))

        return result


if __name__ == '__main__':
    Crawler().collect()
