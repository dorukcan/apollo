import datetime
import logging
import re

from apiclient import discovery

from apollo.backend import BaseCrawler, build_soup, q, qs, qst
from apollo.modules import Trackable

logger = logging.getLogger()
logger.setLevel(logging.ERROR)


class SocialBlade(BaseCrawler):
    def __init__(self):
        super().__init__()

    def collect(self):
        response = self.make_get("https://socialblade.com/youtube/top/country/tr")
        soup = build_soup(response.text)

        url_list = []

        for item in q(soup, 'a'):
            href = item.get('href', '')

            if '/youtube/user/' in href:
                url_list.append('https://socialblade.com' + href)

        responses = self.make_get_async(url_list, name="pages")

        result = []

        for response in responses:
            soup = build_soup(response.text)

            social = {}
            for item in q(soup, "#YouTubeUserTopSocial .core-button"):
                name = qs(item, "i").get('class').replace('fa fa-', '').replace('-play', '')
                url = item.get('href')
                social[name] = url

            details = {}
            for item in q(soup, "#YouTubeUserTopInfoBlock .YouTubeUserTopInfo"):
                label = qst(item, ".YouTubeUserTopLight")
                value = qs(item, "span", -1).text_content().strip()
                details[label] = value

            result.append({
                "title": qst(soup, '#YouTubeUserTopInfoBlockTop h1'),
                "social": social,
                "details": details,
            })

        return result


def str_to_date(target, fmt):
    try:
        return datetime.datetime.strptime(target[:-5], fmt)
    except:
        return target


def flatten_dict(dict_obj, parent_key=""):
    result = {}

    key_black_list = ["thumbnails", "TopicIds", "topicIds", "kind", "etag", "liveBroadcastContent",
                      "favoriteCount", "relevantTopicIds", "hiddenSubscriberCount", "customUrl",
                      "regionRestriction"]

    for key, value in dict_obj.items():
        if key not in key_black_list:
            _key = key.lower()

            if parent_key != "":
                _key = parent_key + "_" + key.lower()

            if key == "duration":
                re_duration = r"T((?P<hours>.*?)H)?((?P<minutes>.*?)M)?((?P<seconds>.*?)S)?"
                tokens = re.search(re_duration, value).groupdict()
                value = int(tokens["hours"] or 0) * 60 * 60
                value += int(tokens["minutes"] or 0) * 60
                value += int(tokens["seconds"] or 0)

            if type(value) is dict:
                child_values = flatten_dict(value, _key)
                result = {**result, **child_values}
            elif type(value) is str:
                if value.isdigit():
                    value = int(value)
                else:
                    value = str_to_date(value, "%Y-%m-%dT%H:%M:%S")
            elif type(value) is list:
                value = [item.replace("https://en.wikipedia.org/wiki/", "") for item in value]

            result[_key] = value

    return result


def flatten_dict_list(dict_list):
    return [flatten_dict(dict_obj) for dict_obj in dict_list]


class YoutubeApi:
    def __init__(self):
        self.service = None

    def _get_service(self):
        if not self.service:
            self.service = discovery.build(
                serviceName='youtube', version='v3',
                developerKey='AIzaSyDaCK1nXsJBFfd3C6x6dx7pzgntg6BztNs'
            )

        return self.service

    def _make_request(self, payload, method, all_pages=False):
        service = self._get_service()
        response = getattr(service, method)().list(**payload).execute()

        if "error" in response:
            return [{"error": response}]

        result = response["items"]

        if all_pages and "nextPageToken" in response:
            next_page = self._make_request(
                payload={**payload, **{"pageToken": response["nextPageToken"]}},
                method=method
            )
            result.extend(next_page)

        return result

    def get_videos(self, id_list):
        result = []

        for start in range(0, len(id_list), 50):
            combined_key = ",".join(id_list[start:start + 50])

            payload = dict(
                part='contentDetails,id,snippet,statistics,topicDetails',
                id=combined_key,
                hl='tr_TR',
                maxResults=50
            )

            result.extend(self._make_request(payload, "videos"))

        return result

    def get_channels(self, id_list):
        result = []

        for start in range(0, len(id_list), 50):
            payload = dict(
                part='snippet,statistics,topicDetails',
                id=",".join(id_list[start:start + 50]),
                hl='tr_TR',
                maxResults=50
            )

            result.extend(self._make_request(payload, "channels"))

        return result

    def get_videos_of_channel(self, channel_id, page_token=None):
        payload = dict(
            part='id',
            channelId=channel_id,
            type='video',
            maxResults=50,
            pageToken=page_token
        )

        return self._make_request(payload, "search", all_pages=True)


class Crawler(BaseCrawler):
    def __init__(self):
        super().__init__()

        self.api = YoutubeApi()

    def collect(self):
        channel_ids = self.get_channel_ids()
        video_ids = self.get_video_ids(channel_ids)

        channels = self.do_channels(channel_ids)
        videos = self.do_videos(video_ids)

        return self.make_records(channels, videos)

    def get_channel_ids(self, limit=10):
        social_blade_data = SocialBlade().collect()

        channel_ids = []

        for item in social_blade_data:
            channel_ids.append(item['social']['youtube'].replace('https://youtube.com/channel/', ''))

        return channel_ids[:limit]

    def get_video_ids(self, channel_ids):
        video_ids = []

        for channel_id in channel_ids:
            video_ids.extend(self.do_video_ids(channel_id))

        return video_ids

    #############################################

    def do_channels(self, channel_ids):
        result = []

        channels_data = self.api.get_channels(channel_ids)
        channels_data = flatten_dict_list(channels_data)

        for item in channels_data:
            result.append({
                "id": item.get("id"),

                "title": item.get("snippet_title"),
                "published_at": item.get("snippet_publishedat"),

                "sub_count": item.get("statistics_subscribercount"),
                "video_count": item.get("statistics_videocount"),
                "view_count": item.get("statistics_viewcount"),
            })

        return result

    def do_videos(self, video_ids):
        result = []

        videos_data = self.api.get_videos(video_ids)
        videos_data = flatten_dict_list(videos_data)

        for item in videos_data:
            result.append({
                "id": item.get("id"),
                "channel_id": item.get("snippet_channelid"),

                "title": item.get("snippet_title"),
                "published_at": item.get("snippet_publishedat"),
                "duration": item.get("contentdetails_duration"),

                "comment_count": item.get("statistics_commentcount"),
                "view_count": item.get("statistics_viewcount"),
                "like_count": item.get("statistics_likecount"),
                "dislike_count": item.get("statistics_dislikecount"),
            })

        return result

    def do_video_ids(self, channel_id):
        video_ids = []

        videos = self.api.get_videos_of_channel(channel_id)

        # maybe error
        for item in videos:
            if "error" in item:
                continue

            video_id = item.get('id')

            if type(video_id) is dict:
                video_id = video_id.get("videoId")

            if video_id:
                video_ids.append(video_id)

        return video_ids

    #############################################

    def make_records(self, channels, videos):
        source = "youtube"
        result = []

        ch_metrics = ["sub_count", "video_count", "view_count"]
        for channel in channels:
            result.extend(self.make_record(channel, "id", None, source, ch_metrics, "channel"))

        vid_metrics = ["comment_count", "view_count", "like_count", "dislike_count"]
        for video in videos:
            result.extend(self.make_record(video, "id", "channel_id", source, vid_metrics, "video"))

        return result

    def make_record(self, item, code, parent_code, source, metrics, type_key):
        return [Trackable(
            code=item[code],
            parent_code=item[parent_code] if parent_code else None,
            source=source,
            metric_name=metric_name,
            metric_value=item[metric_name],
            type_key=type_key,
            created_at=datetime.datetime.now()
        ) for metric_name in metrics]


if __name__ == '__main__':
    Crawler().collect()
