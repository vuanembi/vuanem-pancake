from pancake.pipeline.define import Pancake_statistics
from typing import Any
import time
from datetime import datetime, timedelta
from pancake.pancake_service import get_statistics


def schema() -> list[dict[str, Any]]:
    _schema = [
        {"name": "categories", "type": "STRING"},
        {
            "name": "series",
            "type": "record",
            "mode": "repeated",
            "fields": [
                {"name": "tag", "type": "STRING"},
                {"name": "pin", "type": "INT64"},
                {"name": "total", "type": "INT64"},
            ],
        },
    ]
    return _schema


def transform(rows):
    rows_to_insert = []
    for i in range(0, len(rows["categories"])):
        series = []
        c = rows["categories"]
        for key in rows["series"]:
            tag = key
            t = rows["series"]
            temp = t[key]
            a = temp[i]
            pin = a["pin"]
            total = a["total"]
            my_dict = {"tag": tag, "pin": pin, "total": total}
            series.append(my_dict)
        dict = {
            "categories": c[i],
            "series": series,
        }
        rows_to_insert.append(dict)
    return rows_to_insert


define = Pancake_statistics("tags", "statistics/tags", transform, schema())

