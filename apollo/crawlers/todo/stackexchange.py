import json
from urllib.parse import urlencode

import venus
import venus.objects
import venus.utils


class Stackexchange:
    def __init__(self):
        pass

    def run(self):
        self.collect()

    def collect(self):
        all_sites = self.get_all_site_keys()

        for site_key in all_sites:
            questions = self.get_questions(site_key, max_page=20)
            venus.qb.save_data(questions, "stackexchange.questions")

            venus.utils.sleep_rand(0, 2, debug=True)

    def get_site_questions(self, site_key="stackoverflow"):
        questions = self.get_questions(site_key, max_page=1)

        return questions

    def get_all_site_keys(self):
        response = venus.objects.Request(url="https://api.stackexchange.com/2.2/sites?page=1&pagesize=1000").make()
        data = json.loads(response.output)

        return [
            item["api_site_parameter"]
            for item in data["items"]
            if "meta" not in item["api_site_parameter"]
        ]

    def get_questions(self, site_key, max_page=2):
        questions_pool = [venus.objects.Request(
            url=self.prepare_question_url(site_key, page)
        ) for page in range(1, max_page + 1)]

        questions_raw = venus.objects.RequestPool(pool=questions_pool, name=site_key).make_async()

        questions = [self.extract_question(site_key, response) for response in questions_raw]
        return venus.utils.flatten_array(questions)

    def prepare_question_url(self, site_key, page):
        return "https://api.stackexchange.com/2.2/questions?" + urlencode({
            "pagesize": 100,
            "order": "desc",
            "sort": "votes",
            "filter": "!BHMsVcb_v171zxR-yWCGY4.-EOhBra",
            "page": page,
            "site": site_key,
            "key": "ehNq0f0NiPpyxR4F)VDDJA(("
        })

    def extract_question(self, site_key, response):
        result_obj = json.loads(response.output)

        if "items" in result_obj:
            result = venus.utils.insert_all_dicts(result_obj["items"], {
                "site_key": site_key
            })
        else:
            result = []

        return result


if __name__ == "__main__":
    Stackexchange().run()
