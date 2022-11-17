import pytest
import datetime
import time
from Pancake.get_Page_Statistics import getPageStatistics

@pytest.fixture
def since():
    since=datetime.datetime.now() + datetime.timedelta(days = -5)
    since = int(time.mktime(since.timetuple()))    
    return since

@pytest.fixture
def until():
    until=datetime.datetime.now()
    until = int(time.mktime(until.timetuple()))
    return until

def test_get_page_statistics(since,until):
    getPageStatistics(since,until)
