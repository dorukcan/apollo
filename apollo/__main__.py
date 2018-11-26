import sys

import sentry_sdk

from .backend import Engine
from .settings import SENTRY_URL


def start():
    sentry_sdk.init(SENTRY_URL)
    Engine().start(sys.argv)


if __name__ == '__main__':
    start()
