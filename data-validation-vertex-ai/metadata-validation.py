from kfp.v2 import dsl, compiler
import configparser
from google.cloud import aiplatform


PROJECT_ID = "{YOUR_PROJECT_ID}"
BUCKET_NAME = PROJECT_ID + "-bucket"
PIPELINE_ROOT = "gs://" + "{BUCKET_NAME}/pipeline_root/"
REGION = "{YOUR_REGION}}"


@dsl.component(
    base_image="python:3.9",
    packages_to_install=[
        "pandas==1.3.5",
        "gcsfs==2023.1.0"
    ],
)
def dep_fetch() -> dict:
    # read from csv into a dict, that takes dependency dag name as the key and dependency table names as the values
    # {"dag1": [table1, table2], "dag2": [table3, table4, table5]}
    import pandas as pd
    import logging

    df = pd.read_csv("gs://penny-trial-bucket/data-dependencies.csv", sep="\t", header=0)
    BQ_TABLE_LIST = {}
    for index, row in df.iterrows():
        if row["DAG_dependencies"] not in BQ_TABLE_LIST:
            BQ_TABLE_LIST[row["DAG_dependencies"]] = [row["destination_project"]]
        else:
            BQ_TABLE_LIST[row["DAG_dependencies"]].append(row["destination_project"])
    logging.info("BQ_TABLE_LIST is ", BQ_TABLE_LIST)
    return BQ_TABLE_LIST


@dsl.component(
    base_image="python:3.9",
    packages_to_install=["pytz==2023.3", "google-cloud-bigquery==3.9.0"],
)
def general_test(BQ_TABLE_LIST: dict) -> bool:
    import os
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from datetime import datetime
    import pytz
    
    
    PROJECT_ID = "penny-trial"
    BUCKET_NAME = "gs://" + PROJECT_ID + "-bucket"
    PIPELINE_ROOT = f"{BUCKET_NAME}/pipeline_root/"
    BUCKET_URI = "penny-trial-bucket"
    REGION = "europe-west4"

    client = bigquery.Client(project=PROJECT_ID)

    # define a lambda function to get the delta days
    get_delta_day = lambda td: td.days

    # General checks
    for bq_table_key, bq_tables in BQ_TABLE_LIST.items():
        for bq_table in bq_tables:
            print("Currently checking tables for DAG", bq_table_key)
            table = client.get_table(bq_table)
            print(
                "Got table '{}.{}.{}'.".format(
                    table.project, table.dataset_id, table.table_id
                )
            )
            print("Table schema: {}".format(table.schema))
            print("Table description: {}".format(table.description))
            print("Table has {} rows".format(table.num_rows))
            assert table.num_rows > 0, "Table has no rows"
            assert (
                get_delta_day(datetime.now(pytz.utc) - table.modified) < 30
            ), "Table is not updated in the last 30 days"

    # Table specific checks
    table1 = client.get_table(BQ_TABLE_LIST["table1"])
    # add example assert here
    assert (
        get_delta_day(datetime.now(pytz.utc) - table1.modified) < 3
    ), "Table is not updated in the last 3 days"
    return True

# Initialize the Vertex AI SDK
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET_NAME)

# A simple pipeline that contains a single hello_world task
@dsl.pipeline(
    name='metadata-pipeline')
def metadata_pipeline():
    dep_fetch_task = dep_fetch()
    general_test(dep_fetch_task.output)

# Compile the pipeline and generate a JSON file
compiler.Compiler().compile(pipeline_func=metadata_pipeline,
                            package_path='compiled_metadata_pipeline.json')

job = aiplatform.PipelineJob(
    display_name=f'metadata-pipeline',
    template_path="compiled_metadata_pipeline.json",
    pipeline_root=PIPELINE_ROOT,
    enable_caching=False,
)

job.submit()