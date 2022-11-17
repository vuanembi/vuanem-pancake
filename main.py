import datetime
import time
from load import load
from Pancake.get_Page_Statistics import getPageStatistics

if __name__ == "__main__":
    # _since = input("Since: ")
    # _until = input("Until: ")
    # _since = datetime.datetime.strptime(_since,"%Y-%m-%d %H:%M:%S")
    # _until = datetime.datetime.strptime(_until,"%Y-%m-%d %H:%M:%S")
    _since = datetime.datetime.now() + datetime.timedelta(days = -5)
    _until = datetime.datetime.now()
    _since = int(time.mktime(_since.timetuple()))
    _until = int(time.mktime(_until.timetuple()))
    # getPageStatistics(_since,_until)
    # print(_since)
    # print(_until)
    # print(getPageStatistics(1668358800,1668487570))
    # print(getPageStatistics(1668911204,1668652004))
    data = getPageStatistics(_since, _until)
    # print(data)
    load(data)