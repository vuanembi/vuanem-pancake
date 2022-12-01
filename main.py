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

def main(request):
    request_json = request.get_json()
    since = request_json.get('since')
    until = request_json.get('until')
    print(since, until)
    for pipeline in pipelines:
        pancake_service(pipeline, since, until)
    return 'OK'