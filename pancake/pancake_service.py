import requests
import os
from datetime import datetime, timedelta
import time
from pancake.pipeline.define import Pancake_statistics
from db import bigquery

key = "PANCAKE_TOKEN"
BASE_URL = "https://pages.fm/api/public_api/v1"
page_id = "434513510026090"
access_token = os.getenv(key)


def get_statistics(endpoint, _since, _until):
    _since = datetime.strptime(_since, "%Y-%m-%d")
    _until = datetime.strptime(_until, "%Y-%m-%d")
    _since = int(time.mktime(_since.timetuple()))
    _until = int(time.mktime(_until.timetuple()))
    with requests.Session() as session:
        session.params = {
            "access_token": access_token,
            "since": _since,
            "until": _until,
        }
        request = session.get(f"{BASE_URL}/pages/{page_id}/{endpoint}")
        request_json = request.json()
        _data = request_json["data"]
        return _data


def pancake_service(pipeline: Pancake_statistics, _since, _until):
    result = get_statistics(pipeline.endpoint, _since, _until)
    data = pipeline.transform(result)
    name = pipeline.name
    schema = pipeline.schema
    # print(data)
    bigquery.load(data, schema, name)