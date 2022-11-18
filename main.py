import time
from datetime import datetime, timedelta
from pancake.pipeline import pages
from pancake.get_statistics import snake_case
from db.bigquery import load
from pancake.get_statistics import snake_case
from pancake.pipeline.pages import transform, schema

if __name__ == "__main__":
    # _since = input("Since: ")
    # _until = input("Until: ")
    # _since = datetime.datetime.strptime(_since,"%Y-%m-%d %H:%M:%S")
    # _until = datetime.datetime.strptime(_until,"%Y-%m-%d %H:%M:%S")
    # _since = datetime.datetime.now() + datetime.timedelta(days = -5)
    # _until = datetime.datetime.now()
    # _since = int(time.mktime(_since.timetuple()))
    # _until = int(time.mktime(_until.timetuple()))
    # getPageStatistics(_since,_until)
    # print(_since)
    # print(_until)
    # print(getPageStatistics(1668358800,1668487570))
    # print(getPageStatistics(1668911204,1668652004))
    # since = datetime.now() + timedelta(days = -5)
    # until = datetime.now()
    # since = int(time.mktime(since.timetuple()))
    # until = int(time.mktime(until.timetuple()))
    # name="pages"
    pipeline = pages.define
    data = pipeline.transform
    name = pipeline.name
    schema = pipeline.schema
    # print(data)
    # print(name)
    # print(schema)
    load(data, schema, name)
    # print(snake_case(since,until,name))
    # print(transform(snake_case(since,until,name)))
    # print(schema())