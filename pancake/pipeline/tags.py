from pancake.pipeline.interface import PancakeStatistics
from typing import Any
from datetime import datetime


def schema() -> list[dict[str, Any]]:
    _schema = [
        {"name": "categories", "type": "TIMESTAMP"},
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
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]
    return _schema


def transform(rows, _year):
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
        _categories = c[i] + "/" + str(_year)
        _categories = datetime.strptime(_categories, "%d/%m/%Y")
        _categories = _categories.isoformat(timespec="seconds")
        _dict = {
            "categories": _categories,
            "series": series,
            "_batched_at": datetime.now().isoformat(timespec="seconds"),
        }
        rows_to_insert.append(_dict)
    return rows_to_insert


define = PancakeStatistics("tags", "statistics/tags", transform, schema())
