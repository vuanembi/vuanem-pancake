from google.cloud import bigquery
from typing import Any
import datetime
from datetime import datetime, timedelta
import time


client = bigquery.Client()
table_id = "voltaic-country-280607.IP_Pancake.p_PageStatistics"

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

d  = lambda rows:[
        {
            "customer_comment_count": row["customer_comment_count"],
            "customer_inbox_count": row["customer_inbox_count"],
            "_hour": (datetime.strptime(row["hour"], '%Y-%m-%dT%H:%M:%S') + timedelta(hours = -7)).strftime('%Y/%m/%d %H:%M:%S'),
            "inbox_interactive_count": row["inbox_interactive_count"],
            "new_customer_count": row["new_customer_count"],
            "new_inbox_count": row["new_inbox_count"],
            "page_comment_count": row["page_comment_count"],
            "page_inbox_count": row["page_inbox_count"],
            "phone_number_count": row["phone_number_count"],
            "today_uniq_website_referral": row["today_uniq_website_referral"],
            "today_website_guest_referral": row["today_website_guest_referral"],
            "uniq_phone_number_count": row["uniq_phone_number_count"],
            "_batched_at": datetime.now().strftime('%Y/%m/%d %H:%M:%S'),
        }
        for row in rows
    ]
    

def load(data: list[dict[str, Any]]):
    result = d(data)
    output_rows = (
        client.load_table_from_json(
            result,
            f"{table_id}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=schema(),
            ),
        )
        .result()
    )