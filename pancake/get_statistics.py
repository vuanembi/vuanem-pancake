import requests
import os

key = "PANCAKE_TOKEN"
BASE_URL = "https://pages.fm/api/public_api/v1"
page_id = "434513510026090"
access_token = os.getenv(key)


def snake_case(_since, _until, _name):
    with requests.Session() as session:
        session.params = {
            "access_token": access_token,
            "since": _since,
            "until": _until,
        }
        request = session.get(f"{BASE_URL}/pages/{page_id}/statistics/{_name}")
        request_json = request.json()
        _data = request_json["data"]
        # print(request_json)
        return _data