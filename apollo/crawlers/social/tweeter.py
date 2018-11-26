import datetime

import twitter

from apollo.backend import BaseCrawler
from apollo.modules import Trackable


class Consumer:
    def __init__(self):
        self._api = None

    @property
    def api(self):
        if not self._api:
            self._api = twitter.Api(
                consumer_key="Vs3Lh0hXIZKuvPkVDML3565Qy",
                consumer_secret="rxq4h4x0KR0VtrOKstqygpgKbURVk37NNvKRvofz9Hpe50sx7D",
                access_token_key="1080080046-bsSM8bVdYxZmkXDsDU7TCLHDC2vEKUMKtdt2lKI",
                access_token_secret="3HrpS2oSVHJ4HeRUWiHSRSCm2i9FpgGDgFcjYHYf0Wf5E"
            )

        return self._api

    def _make_response(self, raw_response):
        return [item.AsDict() for item in raw_response]

    def user_lookup(self, screen_name):
        response = self.api.UsersLookup(screen_name=screen_name)
        return self._make_response(response)

    def get_user_followings(self, username):
        response = self.api.GetFriends(screen_name=username, count=200)
        return self._make_response(response)

    def get_user_followers(self, username):
        response = self.api.GetFollowers(screen_name=username, count=200)
        return self._make_response(response)

    def get_user_tweets(self, username):
        output = []
        last_id = None

        while True:
            response = self.api.GetUserTimeline(
                screen_name=username, count=200, include_rts=True,
                trim_user=False, exclude_replies=False, max_id=last_id
            )

            if len(response) == 1:
                break

            output.extend(response)
            last_id = response[-1]["id"]

        return self._make_response(response)


class Crawler(BaseCrawler):
    def __init__(self):
        super().__init__()

        self.consumer = Consumer()

    def collect(self):
        start_user = "luigikentmen"

        followings = self.consumer.get_user_followings(start_user)
        profiles = self.do_profiles(followings)

        result = self.make_records(profiles)

        return result

    def do_profiles(self, raw_data):
        result = []

        for item in raw_data:
            result.append({
                "id": item["id"],
                "screen_name": item.get("screen_name"),

                "followings_count": item.get("friends_count", 0),
                "followers_count": item.get("followers_count", 0),
                "favorites_count": item.get("favourites_count", 0),
                "statuses_count": item.get("statuses_count", 0),
                "listed_count": item.get("listed_count", 0),
                "is_protected": item.get("protected", None),
            })

        return result

    def make_records(self, profiles):
        result = []

        source = "twitter"
        profile_metrics = ["followings_count", "followers_count", "favorites_count", "statuses_count"]

        for profile in profiles:
            result.extend(self.make_record(profile, "id", None, source, profile_metrics, "profile"))

        return result

    def make_record(self, item, code, parent_code, source, metrics, type_key):
        return [Trackable(
            code=item[code],
            parent_code=item[parent_code] if parent_code else None,
            source=source,
            metric_name=metric_name,
            metric_value=item.get(metric_name),
            type_key=type_key,
            created_at=datetime.datetime.now()
        ) for metric_name in metrics]
