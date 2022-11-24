import requests
import os
from datetime import datetime, timedelta, date, timezone
import time
from pancake.pipeline.interface import PancakeStatistics
from db.bigquery import load

BASE_URL = "https://pages.fm/api/public_api/v1"
page_id = "434513510026090"


def str_to_date(since, until):
    try:
        since = datetime.strptime(since, "%Y-%m-%d")
        until = datetime.strptime(until, "%Y-%m-%d")
    except:
        since = datetime.now() + timedelta(days=-5)
        until = datetime.now()
    years = list(range(since.year, until.year + 1))
    if since.year < until.year:
        rows = [
            (
                since.replace(tzinfo=timezone.utc),
                datetime(years[1], 1, 1).replace(tzinfo=timezone.utc),
            ),
            *[
                (
                    datetime(year, 1, 1).replace(tzinfo=timezone.utc),
                    datetime(year + 1, 1, 1).replace(tzinfo=timezone.utc),
                )
                for year in years[1:-1]
            ],
            (
                datetime(years[-1], 1, 1).replace(tzinfo=timezone.utc),
                (until + timedelta(days=1)).replace(tzinfo=timezone.utc),
            ),
        ]
    else:
        rows = [
            (
                since.replace(tzinfo=timezone.utc),
                (until + timedelta(days=1)).replace(tzinfo=timezone.utc),
            )
        ]
    return rows


def get_statistics(endpoint, _since, _until):
    with requests.Session() as session:
        params = {
            "access_token": os.getenv("PANCAKE_TOKEN"),
            "since": _since,
            "until": _until,
        }
        request = session.get(
            f"{BASE_URL}/pages/{page_id}/{endpoint}",
            params=params,
        )
        request_json = request.json()
        _data = request_json["data"]
        return _data


def pancake_service(pipeline: PancakeStatistics, _since, _until):
    rows = str_to_date(_since, _until)
    print(rows)
    for row in rows:
        _year = row[0].year
        _since = int(row[0].timestamp())
        _until = int(row[1].timestamp())
        result = get_statistics(pipeline.endpoint, _since, _until)
        data = pipeline.transform(result, _year)
        name = pipeline.name
        schema = pipeline.schema
        load(data, schema, name)