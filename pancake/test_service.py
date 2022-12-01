import pytest
import datetime
import time
from pancake.pancake_service import get_statistics, str_to_date
from pancake.pipeline.pages import transform
from pancake.pipeline import pages


@pytest.fixture
def data():
    pipeline = pages.define
    data = pipeline.transform(get_statistics(endpoint,since,until),2022)
    return data


@pytest.fixture
def since():
    since = datetime.datetime.now() + datetime.timedelta(days=-5)
    since = int(time.mktime(since.timetuple()))
    return since


@pytest.fixture
def until():
    until = datetime.datetime.now()
    until = int(time.mktime(until.timetuple()))
    return until


@pytest.fixture
def name():
    name = "pages"
    return name


@pytest.fixture
def endpoint():
    endpoint = "statistics/pages"
    return endpoint

@pytest.fixture
def schema():
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


def test_get_statistics(endpoint,since,until):
    get_statistics(endpoint,since,until)


def test_transform(endpoint,since,until):
    transform(get_statistics(endpoint,since,until),2022)

def test_str_to_date(since, until):
    str_to_date(since,until)