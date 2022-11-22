import time
from datetime import datetime, timedelta
from pancake.pipeline import (
    pages,
    tags,
)
from pancake.pancake_service import get_statistics, pancake_service
from db.bigquery import load

pipelines = [
    pages.define,
    tags.define,
]

if __name__ == "__main__":
    since = input("Since: ")
    until = input("Until: ")
    for pipeline in pipelines:
        pancake_service(pipeline, since, until)