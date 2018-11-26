import datetime

from apollo.backend import BaseCrawler, build_soup, make_async, q, qst
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        page_list = self.get_category_list()
        responses = self.make_get_async(page_list, name="pages")

        return make_async(self.extract_category, responses)

    def get_category_list(self):
        url_template = 'https://www.kitapyurdu.com/index.php'
        url_template += '?route=product/category'
        url_template += '&path={path}'
        url_template += '&sort=publish_date'
        url_template += '&order=DESC'
        url_template += '&filter_in_stock=1'
        url_template += '&filter_category_all=true'
        url_template += '&filter_product_type=1'
        url_template += '&limit=10000'

        response = self.make_get_req("http://www.kitapyurdu.com/index.php?route=product/category")
        soup = build_soup(response.text)

        category_list = []

        for item in q(soup, "#content > div > a"):
            category_list.append(item.get('href').split("/")[-1].replace(".html", ""))

        return [url_template.format(path=category) for category in category_list]

    def extract_category(self, response):
        if not response.text:
            return None

        soup = build_soup(response.text)
        category = qst(soup, '.breadcrumb', True).split('»')[-1].strip()

        result = []

        for product in q(soup, '#content .product-grid > div'):
            price_text = qst(product, '.price .price-new .value', True)

            if not price_text:
                price_text = qst(product, '.price .value', True)

            price_text = price_text.replace("Liste Fiyatı:", "").replace(' TL', '')
            price_text = price_text.replace('.', '').replace(',', '.')

            try:
                price_text = float(price_text.strip())
            except:
                continue

            result.append(Product(
                code=product.get('id').replace('product-', ''),
                brand=product.cssselect('.publisher')[0].text_content().strip(),
                price=price_text,
                title=product.cssselect('*[itemprop="name"]')[0].text_content().strip(),
                url=product.cssselect('a[itemprop="url"]')[0].get('href'),
                category=category,
                created_at=datetime.datetime.now()
            ))

        return result
