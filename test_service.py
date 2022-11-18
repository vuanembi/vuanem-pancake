import pytest
import datetime
import time
from pancake.get_statistics import snake_case
from pancake.pipeline.pages import transform, schema
from db.bigquery import load


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


def test_get_statistics(since, until, name):
    snake_case(since, until, name)


def test_transform(since, until, name):
    transform(snake_case(since, until, name))


def test_load(since, until, name):
    load(transform(snake_case(since, until, name)), schema(), name)
