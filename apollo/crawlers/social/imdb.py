import datetime
import re

from bs4 import BeautifulSoup

from apollo.backend import BaseCrawler
from apollo.modules import Trackable


def extract_year_with_paranthesis(year_text):
    try:
        return int(year_text.replace('(', '').replace(')', ''))
    except:
        return None


class Crawler(BaseCrawler):
    def collect(self):
        raw_top250_list = self.make_get(url="https://www.imdb.com/chart/top")
        top_250_list = self.extract_top250_list(raw_top250_list)

        result = []

        source = "imdb"
        type_key = "movie"

        metrics = ["rating", "rating_count"]

        for movie in top_250_list:
            for metric_name in metrics:
                result.append(Trackable(
                    code=movie["movie_id"],
                    parent_code=None,
                    source=source,
                    metric_name=metric_name,
                    metric_value=movie[metric_name],
                    type_key=type_key,
                    created_at=datetime.datetime.now()
                ))

        return result

    def extract_top250_list(self, response):
        soup = BeautifulSoup(response.text, 'lxml')

        docs = []
        re_rating = re.compile('(.*) based on (.*) user ratings')

        for item in soup.select(".chart .lister-list tr"):
            movie_id = item.select('.seen-widget')[0]["data-titleid"]

            year = item.select('.titleColumn .secondaryInfo')[0].text.strip()
            year = extract_year_with_paranthesis(year)

            rating_text = item.select('.ratingColumn strong')[0]["title"]
            rating_tokens = re_rating.match(rating_text)

            rating = float(rating_tokens.group(1).replace(',', '.'))
            rating_count = int(rating_tokens.group(2).replace('.', ''))

            docs.append({
                "movie_id": movie_id,
                "poster_url": item.select('.posterColumn img')[0]["src"],
                "url": "https://www.imdb.com/title/{}".format(movie_id),
                "title": item.select('.titleColumn a')[0].text.strip(),
                "details": item.select('.titleColumn a')[0]["title"],
                "year": year,
                "rating": rating,
                "rating_count": rating_count
            })

        return docs


if __name__ == '__main__':
    Crawler().collect()
