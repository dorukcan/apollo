import datetime
import json

import lxml.html
import venus
import venus.objects
import venus.utils

venus.setup()

http_headers = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "tr-TR,tr;q=0.8,en-US;q=0.6,en;q=0.4",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Content-Type": "application/json;charset=UTF-8",
    "DNT": "1",
    "Host": "service.yemeksepeti.com",
    "Origin": "https://www.yemeksepeti.com",
    "Pragma": "no-cache",
    "Referer": "https://www.yemeksepeti.com/istanbul/hisarustu",
    "User-Agent": "runscope/0.1"
}


def generate_payload(page, token="0ee626878f344efda116d23d67181384"):
    return {
        "ysRequest": {
            "PageNumber": page,
            "PageRowCount": "50",
            "Token": token,
            "CatalogName": "TR_ISTANBUL",
            "Culture": "tr-TR",
            "LanguageId": "tr-TR"
        },
        "searchRequest": {
            "SortField": 1,
            "SortDirection": 0,
            "OpenOnly": False,
            "AreaId": "3e035f69-b22b-4e8b-9e5f-49c7e283217e"
        }
    }


def clean_price(price_text):
    return float(price_text.replace('TL', '').replace(',', '.').strip())


class Yemeksepeti:
    def __init__(self):
        self.use_cache = False
        self.product_pool = []

    def run(self):
        self.collect_restaurants()
        self.collect_products()
        self.remove_duplicates()

    def collect_restaurants(self, page=1, total_page_count=1):
        venus.utils.logger.info('Yemeksepeti # {current} / {total}'.format(current=page, total=total_page_count))

        api_response = venus.objects.Request(
            url="https://service.yemeksepeti.com/YS.WebServices/CatalogService.svc/SearchRestaurants",
            method=venus.objects.Request.METHOD_POST,
            payload=json.dumps(generate_payload(page)),
            http_headers=http_headers,
            use_cache=self.use_cache,
            expire_days=0.5,
        ).make()

        restaurants = self.extract_restaurants(api_response)
        self.collect_products(restaurants)

        venus.qb.save_data(restaurants, "yemeksepeti.restaurants")

        #####################################################################

        # pagination

        total_page_count = json.loads(api_response.output)["d"]["TotalPageCount"]

        if page != total_page_count:
            return self.collect_restaurants(page + 1, total_page_count)

        return restaurants

    def collect_products(self, restaurants=None):
        if restaurants:
            request_pool = [venus.objects.Request(
                url="https://yemeksepeti.com" + restaurant["seo_url"],
                expire_days=0.5,
                use_cache=self.use_cache,
                extra=dict(seo_url=restaurant["seo_url"])
            ) for restaurant in restaurants if restaurant["seo_url"]]

            self.product_pool.extend(request_pool)
        else:
            responses = venus.objects.RequestPool(pool=self.product_pool, name="restaurants").make_async_full()
            products = [self.extract_products(response) for response in responses]

            products = venus.utils.flatten_array(products)
            venus.qb.save_data(products, "yemeksepeti.products", debug=True)

    def extract_restaurants(self, response):
        data = json.loads(response.output)["d"]["ResultSet"]["searchResponseList"]

        restaurants = []

        for item in data:
            created_date = item.get('CreatedDate', '')
            created_date = created_date.replace("/Date(", "").replace("+0300)/", "")
            created_date = datetime.datetime.fromtimestamp(int(created_date) / 1000) if created_date else None

            restaurants.append({
                "seo_url": item["SeoUrl"],
                "created_at": datetime.datetime.now(),

                "display_name": item["DisplayName"],
                "catalog_name": item["CatalogName"],
                "main_cuisine": item.get('MainCuisineName'),
                "created_date": created_date,
                "image_url": item["ImageFullPath"],

                "avg_restaurant_score": item["AvgRestaurantScorePoint"],
                "avg_flavour_score": item["DetailedFlavour"],
                "avg_serving_score": item["DetailedServing"],
                "avg_speed_score": item["DetailedSpeed"],
                "min_delivery_price": item["MinimumDeliveryPrice"],

                "is_super_delivery": item["SuperDelivery"],
                "is_freezone": item["IsFreezoneRestaurant"],
                "is_new": item["IsNew"],
                "is_open": item["IsOpen"],
                "is_open_all_day": item["IsOpenAllDay"],

                "delivery_fee": item["DeliveryFee"],
                "delivery_time": item["DeliveryTime"],
                "cuisine_list": ",".join(item["CuisineNameList"]),
                "payment_methods": ",".join(item["PaymentMethodsList"]),
                "work_hours": item["WorkHoursText"],
            })

        return restaurants

    def extract_products(self, response):
        parser = lxml.html.HTMLParser()
        doc = lxml.html.document_fromstring(response.output, parser=parser)

        products = []

        for box in doc.cssselect(".restaurantDetailBox"):
            group_name = box.cssselect(".head h2")
            group_name = box.text_content().strip() if group_name else None

            group_attrs = box.get('class').replace('restaurantDetailBox', '').replace('None', '')
            group_attrs = group_attrs.strip().split()

            for item in box.cssselect('li'):
                product_available = True
                product_details = item.cssselect(".productName a")

                if not product_details:
                    product_available = False
                    product_details = item.cssselect(".notAvailableProduct a")

                if not product_details:
                    continue

                product_details = product_details[0]

                ########################################

                product_name = item.cssselect(".productName")
                if product_name:
                    product_name = product_name[0].text_content().strip()
                else:
                    product_name = product_details.cssselect("i")

                    if product_name:
                        product_name = product_name[0].get('data-productname')
                    else:
                        product_name = product_details.text.strip()

                ########################################

                product_id = product_details.get("data-product-id", None)
                is_top_sold = product_details.get("data-top-sold-product", None)

                image_url = product_details.cssselect(".ys-icons-foto")
                image_url = image_url[0].get('data-imagepath') if image_url else None

                info_text = item.cssselect(".productInfo")[0].text_content().strip()
                info_text = info_text if info_text != "" else None

                list_price = item.cssselect(".listedPrice")
                list_price = clean_price(list_price[0].text_content()) if list_price else None

                product_price = item.cssselect(".newPrice")
                product_price = clean_price(product_price[0].text_content()) if product_price else None

                products.append({
                    "created_at": datetime.datetime.now(),
                    "product_id": product_id,

                    "product_name": product_name,
                    "group_name": group_name,
                    "group_attrs": group_attrs,
                    "info_text": info_text,
                    "image_url": image_url,

                    "list_price": list_price,
                    "price": product_price,
                    "top_sold_product": is_top_sold,
                    "product_available": product_available,
                })

        products = venus.utils.insert_all_dicts(products, response.extra)

        return products

    def remove_duplicates(self):
        venus.qb.remove_duplicates(table_name="yemeksepeti.restaurants", fields=[
            "seo_url", "avg_restaurant_score", "avg_serving_score", "avg_speed_score", "avg_flavour_score",
            "delivery_fee", "delivery_time", "min_delivery_price", "payment_methods", "work_hours"])

        venus.qb.remove_duplicates(table_name="yemeksepeti.products", fields=[
            "product_id", "price", "top_sold_product", "product_available"])


if __name__ == "__main__":
    Yemeksepeti().run()
