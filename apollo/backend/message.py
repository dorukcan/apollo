import datetime
import json

import attr

from .database import Database


@attr.s(frozen=True)
class Message:
    label = attr.ib()
    content = attr.ib()
    level = attr.ib()
    extra = attr.ib()
    created_at = attr.ib()

    def save(self):
        Database().insert_data(
            table_name="apollo.messages",
            data=attr.asdict(self)
        )

    def std_out(self):
        print(self.label, ":", self.level, ":", self.content, ":", self.created_at)


def make_message(label, content, level="info", extra=None):
    msg = Message(
        label=label,
        content=content,
        level=level,
        extra=json.dumps(extra, defautl=str) if type(extra) is not str else extra,
        created_at=datetime.datetime.now()
    )

    msg.save()
    msg.std_out()
