import requests
import os
from datetime import datetime, timedelta, date
import time
from pancake.pipeline.interface import PancakeStatistics
from db.bigquery import load

BASE_URL = "https://pages.fm/api/public_api/v1"
page_id = "434513510026090"


def str_to_date(since, until):
    rows = []
    try:
        since = datetime.strptime(since, "%Y-%m-%d")
        until = datetime.strptime(until, "%Y-%m-%d")
    except:
        since = datetime.now() + timedelta(days=-5)
        until = datetime.now()
    syear = since.year
    temp = since.year
    uyear = until.year
    if syear < uyear:
        while temp <= uyear:
            if temp == syear:
                start = since
                end = datetime(since.year + 1, 1, 1, 0, 0, 0)
            elif temp == uyear:
                start = datetime(until.year, 1, 1)
                end = until + timedelta(days=1)
            else:
                start = datetime(temp.year, 1, 1)
                end = datetime(temp.year + 1, 1, 1, 0, 0, 0)
            start = start + timedelta(hours=7)
            end = end + timedelta(hours=7)
            my_list = (start, end)
            rows.append(my_list)
            temp = temp + 1
    else:
        since = since + timedelta(hours=7)
        until = until + timedelta(hours=7)
        my_list = (since, until)
        rows.append(my_list)
    return rows


def parse_date(since: str, until: str) -> tuple:
    since = int(time.mktime(since.timetuple()))
    until = int(time.mktime(until.timetuple()))
    return (since, until)


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
        _date = parse_date(row[0], row[1])
        result = get_statistics(pipeline.endpoint, _date[0], _date[1])
        data = pipeline.transform(result, _year)
        name = pipeline.name
        schema = pipeline.schema
        load(data, schema, name)