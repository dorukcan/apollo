from .backend import CrawlTask


def task_market():
    return CrawlTask(
        module_path="apollo.modules.market",
        site_type="Market",
        db_name='market',
        crawlers=[
            "market.a101",
            "market.carrefoursa",
            "market.migros",
            "market.sahibinden",
            "market.bizimtoptan",
            # "market.kitapyurdu",
            # "market.hm",
            # "market.watsons",
            # "market.lcwaikiki",
        ]
    )


def task_social():
    return CrawlTask(
        module_path="apollo.modules.social",
        site_type="Social",
        db_name='social',
        crawlers=[
            "social.play_store",
            "social.youtube",
            "social.tweeter",
            "social.vikipedi",
            "social.imdb",
        ]
    )


"""
def task_trends():
    return CrawlTask(
        module_path="apollo.modules.trend",
        site_type="Trend",
        db_name='trends',
        crawlers=[
            "google_trends",
        ]
    )
"""
