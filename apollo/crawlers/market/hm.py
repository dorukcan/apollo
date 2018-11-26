import datetime

from apollo.backend import BaseCrawler, build_soup, q, qst, qsu, qs, make_async
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        response = self.make_get_req(url="https://www2.hm.com/tr_tr/index.html")
        soup = build_soup(response.text)
        category_list = [
            "https://www2.hm.com" + item.get('href', '') + '?offset=0&page-size=10000'
            for item in q(soup, '.primary-menu .primary-menu-category a')
        ]

        responses = self.make_get_async(category_list)
        return make_async(self.extract_page, responses)

    def extract_page(self, response):
        if not response.text:
            return []

        result = []

        soup = build_soup(response.text)
        category = qst(soup, 'h1.heading', True)

        for item in q(soup, '.hm-product-item'):
            if qs(item, 'a') is None:
                continue

            price = qst(item, '.item-price .sale', True)

            if not price or price == '':
                price = qst(item, '.item-price .regular', True)

            try:
                price = float(price.replace('.', '').replace(',', '.').replace(' TL', ''))
            except:
                continue

            result.append(Product(
                code=qsu(item, '.item-heading a'),
                brand=None,
                price=price,
                title=qst(item, '.item-heading a', True),
                category=category,
                url="https://www2.hm.com" + qs(item, 'a').get('href', ''),
                created_at=datetime.datetime.now()
            ))

        return result
