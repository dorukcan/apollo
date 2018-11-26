import importlib
import json
from inspect import getmembers, isfunction

import attr

from .exceptions import SiteError
from .message import make_message
from .utils import make_async


class Engine:
    def __init__(self):
        self.prefix = "task_"
        self.task_registry = {}
        self.detect_tasks()

    def start(self, cli_args):
        task_names = cli_args[1:]

        if len(task_names) == 0:
            return self.make_all()

        return self.make_some(task_names)

    def make_all(self):
        task_names = list(self.task_registry.keys())
        self.do_tasks(task_names)

    def make_some(self, task_names):
        self.do_tasks(task_names)

    #################################

    def detect_tasks(self):
        module = importlib.import_module("apollo.tasks")
        fn_names = [o[0] for o in getmembers(module) if isfunction(o[1])]

        for fn_name in fn_names:
            task_name = fn_name.replace(self.prefix, '')
            self.task_registry[task_name] = getattr(module, fn_name)()

    def do_tasks(self, name_list):
        return make_async(self.do_task, name_list, False)

    def do_task(self, task_name):
        # make_message("engine_task", "start", "info", extra=task_name)
        task = self.task_registry.get(task_name)

        try:
            make_message("engine_task", "started", "info", extra=task_name)
            task.run()
            make_message("engine_task", "success", "info", extra=task_name)
        except Exception as e:
            make_message("engine_task", "failed", "error", extra=json.dumps(e, default=str))
            raise e
        finally:
            # make_message("engine_task", "done", "info", extra=task_name)
            pass


@attr.s(slots=True)
class Task:
    module_path = attr.ib()
    site_type = attr.ib()
    db_name = attr.ib()

    def run(self):
        raise NotImplementedError


@attr.s
class CrawlTask(Task):
    crawlers = attr.ib()

    def run(self):
        return make_async(self.do_module, self.crawlers, False)

    def do_module(self, crawler_name):
        try:
            module = importlib.import_module(self.module_path)
            site = getattr(module, self.site_type)(crawler_name=crawler_name)
            site.collect()
        except SiteError:
            make_message("crawl_task", "failed", "error", extra=crawler_name)
