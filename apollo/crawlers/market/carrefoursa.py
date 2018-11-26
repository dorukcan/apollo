import datetime

from apollo.backend import BaseCrawler, build_soup, q, qs, qsu
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        page_list = self.get_category_list()
        responses = self.make_get_async(page_list, name="pages")

        result = []

        for response in responses:
            soup = build_soup(response.text)

            for product in q(soup, '.product-listing .product-card'):
                result.append(Product(
                    code=qsu(product, 'a'),
                    brand=qs(product, '#productBrandNamePost').get('value'),
                    price=float(qs(product, '#productPricePost').get('value')),
                    title=qs(product, '#productNamePost').get('value'),
                    category=qs(product, '#productMainCategoryPost').get('value'),
                    url="https://www.carrefoursa.com" + qsu(product, 'a'),
                    created_at=datetime.datetime.now()
                ))

        return result

    def get_category_list(self):
        response = self.make_get_req(url="https://www.carrefoursa.com")
        soup = build_soup(response.text)

        return ["https://www.carrefoursa.com" + item.get('href') + "?show=All"
                for item in q(soup, '.s-menu-1 > li > a')]
