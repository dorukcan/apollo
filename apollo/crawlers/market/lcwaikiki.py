import datetime

from apollo.backend import BaseCrawler, build_soup, make_async, q, qst, qsu
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        response = self.make_get_req(url="https://www.lcwaikiki.com")
        soup = build_soup(response.text)

        category_list = [
            "https://www.lcwaikiki.com" + item.get('href', '') + '?PageSize=-1&PageIndex=1'
            for item in q(soup, '.navbar-nav a')
            if "/urun-grubu/" in item.get('href', '')
        ]

        responses = self.make_get_async(category_list)
        return make_async(self.extract_page, responses)

    def extract_page(self, response):
        if not response.text:
            return []

        result = []

        soup = build_soup(response.text)
        category = qst(soup, '.category-banner h1', True)

        for item in q(soup, '.c-items .c-item'):
            price = qst(item, '.c-item-price', True)

            try:
                price = float(price.replace('.', '').replace(',', '.').replace(' TL', ''))
            except:
                continue

            result.append(Product(
                code=qsu(item, 'a'),
                brand=None,
                title=qst(item, '.c-item-name', True),
                url="https://www.watsons.com.tr" + qsu(item, 'a'),
                price=price,
                category=category,
                created_at=datetime.datetime.now()
            ))

        return result
