import time
from datetime import datetime, timedelta
from pancake.pipeline import pages
from pancake.pancake_service import get_statistics
from db.bigquery import load
from pancake.pipeline.pages import transform, schema

if __name__ == "__main__":
    pipeline = pages.define
    data = pipeline.transform
    name = pipeline.name
    schema = pipeline.schema
    load(data, schema, name)