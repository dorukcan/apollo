import json

import venus
import venus.utils
from InstagramAPI import InstagramAPI

venus.setup()

IG_USER = 'USERNAME'
IG_PASS = 'PASSWORD'


def cached_function(fn):
    def wrapper(*args, **kwargs):
        _args = args

        if len(_args) > 0 and str(type(_args[0])).startswith("<class"):
            _args = _args[1:]

        cache_key = "{fn_name}-{args}-{kwargs}".format(
            fn_name=fn.__name__,
            args=json.dumps(_args),
            kwargs=json.dumps(kwargs)
        )

        result = venus.cache.get_cache(cache_key)

        if not result:
            result = fn(*args, **kwargs)
            venus.cache.save_cache(cache_key, result)
        else:
            result = json.loads(result)

        return result

    return wrapper


class Api:
    def __init__(self):
        self.api = None

    def init_api(self):
        if not self.api:
            self.api = InstagramAPI(IG_USER, IG_PASS)
            self.api.login()

    @cached_function
    def get_user_by_username(self, username):
        self.init_api()
        self.api.searchUsername(username)

        return self.api.LastJson["user"]

    @cached_function
    def get_user_followings(self, pk, max_id):
        self.init_api()
        self.api.getUserFollowings(pk, max_id)

        return self.api.LastJson

    @cached_function
    def get_user_followers(self, pk, max_id):
        self.init_api()
        self.api.getUserFollowers(pk, max_id)

        return self.api.LastJson

    def api_get_user_posts(self, pk):
        cache_key = "instagram-user_posts-{}".format(pk)
        result = venus.cache.get_cache(cache_key)

        if not result:
            self.init_api()

            result = []
            next_max_id = ''

            while True:
                self.api.getUserFeed(pk, next_max_id)
                temp = self.api.LastJson

                if "status" in temp and temp["status"] == 'fail':
                    break

                if "items" in temp:
                    result.extend(temp["items"])

                if "more_available" in temp and temp["more_available"] is False:
                    break

                next_max_id = temp["next_max_id"]

            venus.cache.save_cache(cache_key, result)
        else:
            result = json.loads(result)

        comments = [item["preview_comments"] for item in result if "preview_comments" in item]
        comments = venus.utils.flatten_array(comments)
        comments = venus.utils.flatten_dict_array(comments)

        posts = venus.utils.flatten_dict_array(result, keys_to_ignore=["preview_comments"])

        return posts, comments


class InstagramSpider:
    def __init__(self):
        self.api = Api()

    def run(self):
        # self.collect_user("dorukcankisin")

        # self.collect_user("bugusto")
        self.crawl_users()

    def crawl_users(self):
        users = venus.db.run_query("""
            SELECT relations.from_user
            FROM instagram.relations
            LEFT JOIN instagram.users ON users.pk = relations.from_pk
            WHERE users.pk ISNULL
            LIMIT 10
        """)

        for user in users:
            print("\n--------------------------------")
            print(user["from_user"], "started")
            self.collect_user(user["from_user"])
            print("--------------------------------\n")

    def collect_user(self, username):
        # get user
        user = self.api.get_user_by_username(username)
        user = venus.utils.flatten_dict(user)
        print("user ok")

        # extract user pk
        pk = user["pk"]

        # get followers
        followers = self.get_user_total_followings(pk)
        followers = [venus.utils.flatten_dict(user) for user in followers]
        print("followers ok,", len(followers))

        # get followings
        followings = self.get_user_total_followers(pk)
        followings = [venus.utils.flatten_dict(user) for user in followings]
        print("followings ok,", len(followings))

        # extract relations
        follower_relations = [
            {"from_pk": _user["pk"], "to_pk": pk, "from_user": _user["username"], "to_user": user["username"]}
            for _user in followers]

        following_relations = [
            {"from_pk": pk, "to_pk": _user["pk"], "from_user": user["username"], "to_user": _user["username"]}
            for _user in followings]

        print("relations ok,", len(follower_relations), len(following_relations))

        self.save_data(user, "instagram.users", keys=["pk"])
        self.save_data(follower_relations, "instagram.relations", keys=["from_pk", "to_pk"])
        self.save_data(following_relations, "instagram.relations", keys=["from_pk", "to_pk"])

        # posts, comments = self.api_get_user_posts(pk)
        # venus.qb.save_data(posts, "instagram.posts")
        # venus.qb.save_data(comments, "instagram.comments")

        return username

    ################################################

    def get_user_total_followings(self, pk):
        followers = []
        next_max_id = ''

        while True:
            temp = self.api.get_user_followings(pk, next_max_id)

            for item in temp["users"]:
                followers.append(item)

            if temp["big_list"] is False:
                return followers

            next_max_id = temp["next_max_id"]

    def get_user_total_followers(self, pk):
        followers = []
        next_max_id = ''
        while 1:
            temp = self.api.get_user_followers(pk, next_max_id)

            for item in temp["users"]:
                followers.append(item)

            if temp["big_list"] is False:
                return followers
            next_max_id = temp["next_max_id"]

    ################################################

    def save_data(self, data, table_name, keys=None):
        data = [data] if data and type(data) is not list else data
        keys = [] if keys is None else keys

        create_query = venus.qb.dict_to_create_table_query(table_name, data)
        venus.db.run_query(create_query)

        previous_query = venus.qb.list_to_select_query(keys, table_name)
        previous_data = venus.db.run_query(previous_query)
        previous_hashes = []

        for previous_item in previous_data:
            previous_hashes.append("-".join([str(previous_item[key]) for key in keys]))

        result = []
        for current_item in data:
            current_hash = "-".join([str(current_item[key]) for key in keys])

            if current_hash not in previous_hashes:
                result.append(current_item)

        venus.qb.save_data(result, table_name, do_single=True, debug=True)

    ################################################

    def remove_duplicates(self):
        self.remove_duplicate("instagram.users")
        self.remove_duplicate("instagram.relations")

    def remove_duplicate(self, table_name):
        pass

    ################################################

    def analyze(self):
        query = """
            WITH my_profile AS (
                SELECT
                  pk,
                  username
                FROM instagram.users
                WHERE username = 'dorukcankisin' AND follower_count NOTNULL
            ),
                my_followings AS (
                  SELECT
                    my_profile.username AS from_user,
                    to_users.username   AS to_user,
                    from_pk,
                    to_pk
                  FROM instagram.relations
                    INNER JOIN my_profile ON my_profile.pk = relations.from_pk
                    INNER JOIN instagram.users to_users ON to_users.pk = relations.to_pk
              )
            
            SELECT
              from_user,
              to_user,
              users.username AS to_to_user,
              relations.to_pk AS to_to_pk
            FROM my_followings
              INNER JOIN instagram.relations ON relations.from_pk = my_followings.to_pk
              INNER JOIN instagram.users ON users.pk = relations.to_pk
            ORDER BY to_user
        """


if __name__ == "__main__":
    InstagramSpider().run()
