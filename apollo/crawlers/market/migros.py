import datetime

import lxml.html
from bs4 import BeautifulSoup

from apollo.backend import BaseCrawler
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        categories = self.list_categories()
        return self.collect_categories(categories)

    def list_categories(self):
        response = self.make_get_req(url='https://www.sanalmarket.com.tr')
        soup = BeautifulSoup(response.text, 'lxml')

        url_list = ["https://www.sanalmarket.com.tr" + item.get('href') for item in
                    soup.select('.category-list-item a')]
        responses = self.make_get_async(url_list=url_list, name="categories")

        result = []

        for response in responses:
            parser = lxml.html.HTMLParser()
            doc = lxml.html.document_fromstring(response.text, parser=parser)

            pages = [int(item.get('data-page', 1)) for item in doc.cssselect('.pagination a[data-page]')]

            result.append({
                "url": response.url,
                "last_page": max(pages) if pages else 1,
            })

        return result

    def collect_categories(self, categories):
        url_list = []

        for category in categories:
            for page in range(1, category["last_page"] + 1):
                url_list.append(category["url"] + "?sayfa=" + str(page))

        responses = self.make_get_async(url_list=url_list, name='pages')

        result = []
        unique_keys = []

        parser = lxml.html.HTMLParser()

        for response in responses:
            doc = lxml.html.document_fromstring(response.text, parser=parser)

            breadcrumb = [item.text_content().strip() for item in doc.cssselect('.breadcrumb span')]
            created_at = datetime.datetime.now()

            for product in doc.cssselect('.sub-category-product-list .product-card'):
                code = product.cssselect('.product_id')[0].get('value')

                if code in unique_keys:
                    continue

                price = product.cssselect('.product-link')[0].get('data-monitor-price')
                price = float(price.replace(',', '.'))

                result.append(Product(
                    code=product.cssselect('.product-link')[0].get('href'),
                    price=price,
                    brand=product.cssselect('.product-link')[0].get('data-monitor-brand'),
                    url='https://www.sanalmarket.com.tr' + product.cssselect('.product-link')[0].get('href'),
                    title=product.cssselect('.product-link img')[0].get('alt'),
                    category=breadcrumb[0],
                    created_at=created_at
                ))

                unique_keys.append(code)

        return result
