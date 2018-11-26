import logging
import os
from concurrent.futures import ThreadPoolExecutor

BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
ETC_DIR = os.path.join(BASE_DIR, "etc")


def merge_into_one(dict_list):
    result = {}

    for item in dict_list:
        result = {**result, **item}

    return result


def set_logger(log_name='market'):
    libs = ["requests", "urllib3"]

    for lib in libs:
        logging.getLogger(lib).setLevel(logging.ERROR)

    #####################################################################

    logging.basicConfig(level=logging.DEBUG, datefmt='%H:%M:%S', format='%(asctime)s %(levelname)s: %(message)s')

    return logging.getLogger(log_name)


def make_async(fn, pool, collect=True):
    result = []

    max_workers = min(len(pool), 4)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_obj = {executor.submit(fn, item): item for item in pool}

        for future in future_to_obj:
            response = future.result()

            if collect and response:
                result.extend(response)

    return result
