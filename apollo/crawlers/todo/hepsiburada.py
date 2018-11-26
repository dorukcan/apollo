import json
import re

import venus
import venus.objects
import venus.utils
from bs4 import BeautifulSoup
from venus.modules import Cache

venus.setup()


def clean_float(price_text):
    price_text = price_text.replace('.', '').replace(',', '.')
    price_text = re.sub('[^0-9.,]', '', price_text)

    return float(price_text)


def flatten_recursive(data, next_key="children"):
    if type(data) is not list:
        data = [data]

    result = []

    for item in data:
        if len(item[next_key]) > 0:
            result.extend(flatten_recursive(item[next_key], next_key=next_key))

        item[next_key] = None
        result.append(item)

    return result


def flatten_recursive2(data, next_key="children"):
    pool = data
    result = []

    while len(pool) > 0:
        item = data.pop()

        if len(item[next_key]) > 0:
            pool.extend(item[next_key])

        item[next_key] = None
        result.append(item)

    return result


class HepsiburadaV2:
    def __init__(self):
        self.products = []

    def run(self):
        self.collect()

    def collect(self):
        category_urls = self.get_category_urls()
        return self.crawl_categories(category_urls)

    def get_category_urls(self):
        response = venus.objects.Request(url="https://hepsiburada.com/").make()
        soup = BeautifulSoup(response.output, 'lxml')

        category_ids = [item["data-itemid"] for item in soup.select('.browser-by-category .menu-main-item')]

        pool = [venus.objects.Request(
            url="https://www.hepsiburada.com/navigation/" + cat_id
        ) for cat_id in category_ids]
        responses = venus.objects.RequestPool(pool=pool, name="categories").make_async_full()

        category_urls = []

        for response in responses:
            data = json.loads(response.output)["items"]
            category_urls.extend([
                "https://www.hepsiburada.com" + item["url"] for item in flatten_recursive2(data) if item["url"]
            ])

        return category_urls

    def crawl_categories(self, category_urls):
        pool = set(category_urls[0:10])
        products = []

        while len(pool) > 0:
            current_pool = [venus.objects.Request(url=url, debug=True) for url in pool]
            responses = venus.objects.RequestPool(pool=current_pool, name="pool").make_async_full()
            pool = set()

            for response in responses:
                if "captcha.hepsiburada.com" in response.final_url:
                    Cache.delete(response.request.url)
                    continue

                category_url = re.sub('\?sayfa=.*?', '', response.final_url)
                soup = BeautifulSoup(response.output, 'lxml')

                for item in soup.select('.search-item .product'):
                    image_url = item.select('.product-image')
                    image_url = image_url[0]["src"] if image_url else None

                    rating = item.select('.product-detail .ratings')
                    rating = rating[1]["style"] if rating else None
                    rating = int(rating.replace('width: ', '').replace('%;', '')) / 20 if rating else None

                    number_of_reviews = item.select('.product-detail .number-of-reviews')
                    number_of_reviews = number_of_reviews[0].text.strip() if number_of_reviews else None
                    number_of_reviews = int(
                        number_of_reviews.replace('(', '').replace(')', '')) if number_of_reviews else None

                    products.append({
                        "url": "https://hepsiburada.com" + item.select('a')[0]["href"],
                        "sku": item.select('a')[0]["data-sku"],
                        "image_url": image_url,
                        "title": item.select('.product-detail .product-title')[0]["title"],
                        "rating": rating,
                        "number_of_reviews": number_of_reviews,
                    })

                last_page = soup.select('.pagination li *')
                if last_page:
                    last_page = int(last_page[-1]["class"][0].replace('page-', ''))
                    pool.update([category_url + '?sayfa=' + str(page) for page in range(2, last_page + 1)])

        return products


if __name__ == '__main__':
    HepsiburadaV2().run()

"""

"price": {
    "discount": item.select('.discount-badge')[0].text().strip(),
    "old": item.select('.product-old-price del')[0].text().strip(),
    "old2": item.select('.product-old-price span')[0].text().strip(),
    "green_text": item.select('.last-price .green-text')[0].text().strip(),
    "last": item.select('.last-price .price-value')[0].text().strip(),
},
"badges": [{
    "label": badge["class"].replace('badge ', ''),
    "value": badge.text().strip()
} for badge in soup.select('.badge-container .badge')]
"""
