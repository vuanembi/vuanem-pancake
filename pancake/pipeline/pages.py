from pancake.pipeline.interface import PancakeStatistics
from typing import Any
import time
from datetime import datetime, timedelta
from pancake.pancake_service import get_statistics


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


def transform(rows,_year):
    return [
        {
            "customer_comment_count": row.get("customer_comment_count"),
            "customer_inbox_count": row.get("customer_inbox_count"),
            "_hour": (
                datetime.strptime(row.get("hour"), "%Y-%m-%dT%H:%M:%S")
                + timedelta(hours=-7)
            ).isoformat(timespec="seconds"),
            "inbox_interactive_count": row.get("inbox_interactive_count"),
            "new_customer_count": row.get("new_customer_count"),
            "new_inbox_count": row.get("new_inbox_count"),
            "page_comment_count": row.get("page_comment_count"),
            "page_inbox_count": row.get("page_inbox_count"),
            "phone_number_count": row.get("phone_number_count"),
            "today_uniq_website_referral": row.get("today_uniq_website_referral"),
            "today_website_guest_referral": row.get("today_website_guest_referral"),
            "uniq_phone_number_count": row.get("uniq_phone_number_count"),
            "_batched_at": datetime.now().isoformat(timespec="seconds"),
        }
        for row in rows
    ]


define = PancakeStatistics("pages", "statistics/pages", transform, schema())
