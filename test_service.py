import pytest
import datetime
import time
from pancake.pancake_service import get_statistics
from pancake.pipeline.pages import transform, schema
from db.bigquery import load
from pancake.pipeline import pages


@pytest.fixture
def data():
    pipeline = pages.define
    data = pipeline.transform
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

def test_get_statistics(endpoint):
    get_statistics(endpoint)


def test_transform():
    transform(get_statistics("statistics/pages"))


def test_load(data, name):
    load(data, schema(), name)