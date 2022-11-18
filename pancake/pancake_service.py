import requests
import os
from datetime import datetime, timedelta
import time

key = "PANCAKE_TOKEN"
BASE_URL = "https://pages.fm/api/public_api/v1"
page_id = "434513510026090"
access_token = os.getenv(key)

_since = datetime.now() + timedelta(days=-5)
_until = datetime.now()
_since = int(time.mktime(_since.timetuple()))
_until = int(time.mktime(_until.timetuple()))


def get_statistics(endpoint):
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
