import datetime
import json

from apollo.backend import BaseCrawler
from apollo.modules import Product


class Crawler(BaseCrawler):
    def collect(self):
        response = self.make_get_req(url="https://www.bizimtoptan.com.tr/core/api/categories/menu")
        data = json.loads(response.text)["payload"]["menu"]
        id_list = [item["id"] for item in data]

        url_template = "https://www.bizimtoptan.com.tr/core/api/categories/{}/products?limit=1000000&order=ovd&min_price[]=0&max_price[]=10000"

        result = []

        for category_id in id_list:
            category_url = url_template.format(category_id)
            response = self.make_get(category_url)
            data = json.loads(response.text)["payload"]["products"]

            for item in data:
                if not item.get("slug"):
                    continue

                result.append(Product(
                    code=item["slug"],
                    brand=item.get('brand'),
                    price=item["price"].get('original'),
                    title=item.get('product_name'),
                    category=item.get('category_breadcrumb'),
                    url="https://www.bizimtoptan.com.tr" + "/" + item["slug"],
                    created_at=datetime.datetime.now()
                ))

        return result
