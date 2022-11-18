from pancake.pipeline.define import Pancake_statistics
from typing import Any
import time
from datetime import datetime, timedelta
from pancake.get_statistics import snake_case


def schema() -> list[dict[str, Any]]:
    _schema = [
        {"name": "customer_comment_count", "type": "INT64"},
        {"name": "customer_inbox_count", "type": "INT64"},
        {"name": "_hour", "type": "TIMESTAMP"},
        {"name": "inbox_interactive_count", "type": "INT64"},
        {"name": "new_customer_count", "type": "INT64"},
        {"name": "new_inbox_count", "type": "INT64"},
        {"name": "page_comment_count", "type": "INT64"},
        {"name": "page_inbox_count", "type": "INT64"},
        {"name": "phone_number_count", "type": "INT64"},
        {"name": "today_uniq_website_referral", "type": "INT64"},
        {"name": "today_website_guest_referral", "type": "INT64"},
        {"name": "uniq_phone_number_count", "type": "INT64"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]
    return _schema


def transform(rows):
    rows_to_insert = []
    for row in rows:
        customer_comment_count = row["customer_comment_count"]
        customer_inbox_count = row["customer_inbox_count"]
        _hour = (
            datetime.strptime(row["hour"], "%Y-%m-%dT%H:%M:%S") + timedelta(hours=-7)
        ).strftime("%Y/%m/%d %H:%M:%S")
        inbox_interactive_count = row["inbox_interactive_count"]
        new_customer_count = row["new_customer_count"]
        new_inbox_count = row["new_inbox_count"]
        page_comment_count = row["page_comment_count"]
        page_inbox_count = row["page_inbox_count"]
        phone_number_count = row["phone_number_count"]
        today_uniq_website_referral = row["today_uniq_website_referral"]
        today_website_guest_referral = row["today_website_guest_referral"]
        uniq_phone_number_count = row["uniq_phone_number_count"]
        _batched_at = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        my_dictionary = {
            "customer_comment_count": customer_comment_count,
            "customer_inbox_count": customer_inbox_count,
            "_hour": _hour,
            "inbox_interactive_count": inbox_interactive_count,
            "new_customer_count": new_customer_count,
            "new_inbox_count": new_inbox_count,
            "page_comment_count": page_comment_count,
            "page_inbox_count": page_inbox_count,
            "phone_number_count": phone_number_count,
            "today_uniq_website_referral": today_uniq_website_referral,
            "today_website_guest_referral": today_website_guest_referral,
            "uniq_phone_number_count": uniq_phone_number_count,
            "_batched_at": _batched_at,
        }
        rows_to_insert.append(my_dictionary)
    return rows_to_insert


_since = datetime.now() + timedelta(days=-5)
_until = datetime.now()
_since = int(time.mktime(_since.timetuple()))
_until = int(time.mktime(_until.timetuple()))

define = Pancake_statistics(
    "pages", transform(snake_case(_since, _until, "pages")), schema()
)
