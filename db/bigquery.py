from google.cloud import bigquery
from typing import Any


client = bigquery.Client()
table_id = "voltaic-country-280607.IP_Pancake."


def load(data: list[dict[str, Any]], schema: list[dict[str, Any]], name):
    output_rows = client.load_table_from_json(
        data,
        f"{table_id}p_{name}Statistics",
        job_config=bigquery.LoadJobConfig(
            create_disposition="CREATE_IF_NEEDED",
            write_disposition="WRITE_APPEND",
            schema=schema,
        ),
    ).result()