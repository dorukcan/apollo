from concurrent.futures import ThreadPoolExecutor

from requests import Session
from tqdm import tqdm

from .cache import Cache

REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "DNT": "1",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


class BaseCrawler:
    def __init__(self):
        self.debug = False

        self.session = Session()
        self.cache = None

    def collect(self):
        raise NotImplementedError

    def make_get_req(self, url, **kwargs):
        if self.debug:
            if not self.cache:
                self.cache = Cache()

            response = self.cache.get(url)

            if response:
                return response

            response = self.make_get(url, **kwargs)

            if response.status_code == 200:
                self.cache.put(url, response)

            return response

        return self.make_get(url, **kwargs)

    def make_get(self, url, **kwargs):
        default_kwargs = dict(
            headers=REQUEST_HEADERS,
            timeout=60,
            allow_redirects=True
        )

        return self.session.request(
            method='GET',
            url=url,
            **{**default_kwargs, **kwargs}
        )

    def make_get_async(self, url_list, name=None, **kwargs):
        output = []

        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_obj = {executor.submit(self.make_get_req, url, **kwargs): url for url in url_list}

            iterator = tqdm(future_to_obj, desc=name) if self.debug is True else future_to_obj

            for future in iterator:
                try:
                    response = future.result()
                except:
                    break

                output.append(response)

        return output
