import datetime

from apollo.backend import BaseCrawler, build_soup, q, qst, qsu, qs, make_async
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        response = self.make_get_req(url="https://www.watsons.com.tr/")
        soup = build_soup(response.text)

        category_list = [
            "https://www.watsons.com.tr" + item.get('href', '') + '?pagesize=120&orderby=20&pagenumber=1'
            for item in q(soup, '.header-menu.desktop .MainMenu .block-category-navigation a')
            if item.get('href', '').startswith('/c/')
        ]

        responses = self.make_get_async(category_list)
        return make_async(self.extract_page, responses)

    def extract_page(self, response):
        if not response.text:
            return []

        result = []

        soup = build_soup(response.text)
        category = qst(soup, '.page-title', True)

        for item in q(soup, '.product-grid .product-item'):
            price = qst(item, '.prices .actual-price', True)

            try:
                price = float(price.replace('.', '').replace(',', '.').replace(' ₺', ''))
            except:
                continue

            result.append(Product(
                code=qsu(item, '.details a'),
                brand=qst(item, '.details .product-title', True),
                title=qst(item, '.details .description', True),
                url="https://www.watsons.com.tr" + qsu(item, '.details a'),
                price=price,
                category=category,
                created_at=datetime.datetime.now()
            ))

        """
        total_item_count = qst(soup, '.product-counts-info span', True).replace(' adet ürün bulundu', '').strip()
        total_page_count = 1

        if total_item_count != '':
            total_page_count = int(int(total_item_count) / 120) + 1
        """

        return result
