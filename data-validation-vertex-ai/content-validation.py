import os
from kfp.v2.google.client import AIPlatformClient
from kfp.v2 import dsl, compiler
from kfp.v2.dsl import Dataset, Input


PROJECT_ID = "{YOUR_PROJECT_ID}"
BUCKET_NAME = "gs://" + PROJECT_ID + "-bucket"
PIPELINE_ROOT = f"{BUCKET_NAME}/pipeline_root/"
REGION = "{YOUR_REGION}}"


@dsl.component(
    base_image="python:3.9",
    packages_to_install=[
        "great_expectations==0.15.32",
        "fsspec==2023.1.0",
        "gcsfs==2023.1.0",
    ],
)
def add_datasource() -> bool:
    import great_expectations as gx
    from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest

    from ruamel import yaml
    import logging

    context = gx.data_context.DataContext(
        context_root_dir="/gcs/penny-trial-bucket/great_expectations"
    )

    datasource_yaml = rf"""
    name: my_datasource
    class_name: Datasource
    execution_engine:
        class_name: PandasExecutionEngine
    data_connectors:
        default_runtime_data_connector_name:
            class_name: RuntimeDataConnector
            batch_identifiers:
                - default_identifier_name
        default_inferred_data_connector_name:
            class_name: InferredAssetGCSDataConnector
            bucket_or_name: penny-trial-bucket
            prefix: great_expectations_test_csvs
            default_regex:
                pattern: (.*)\.csv
                group_names:
                    - data_asset_name
    """

    context.add_datasource(**yaml.load(datasource_yaml))
    # Test the connection
    context.test_yaml_config(yaml_config=datasource_yaml)
    logging.info("Data Source updated")

    batch_request = RuntimeBatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="test-data1.csv",  # this can be anything that identifies this data_asset for you
        runtime_parameters={
            "path": "gs://penny-trial-bucket/great_expectations_test_csvs"
        },  # Add your GCS path here.
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    batch_request.runtime_parameters[
        "path"
    ] = f"gs://penny-trial-bucket/great_expectations_test_csvs/test-data1.csv"
    return True


@dsl.component(
    base_image="python:3.9", packages_to_install=["great_expectations==0.15.32"]
)
def create_test_suite() -> bool:
    from ruamel import yaml
    import great_expectations as gx
    from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest

    context = gx.data_context.DataContext(
        context_root_dir="/gcs/penny-trial-bucket/great_expectations"
    )

    batch_request = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="<YOUR_DATA_ASSET_NAME>",
    )

    batch_request.data_asset_name = "great_expectations_test_csvs/test-data1"

    suite = context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    print(validator.head())

    # Expect all `customer_id` values to not be null
    validator.expect_column_values_to_not_be_null(column="seq")

    # Expect all `unique_key` values to be unique
    validator.expect_column_values_to_be_unique(column="name/first")

    # # Expect `taxi_trip_in_seconds` values to be greater than 0
    validator.expect_column_values_to_be_between(
        column="age", min_value=0, max_value=150
    )

    # And save the final state to JSON
    validator.save_expectation_suite(discard_failed_expectations=False)
    return True


@dsl.component(
    base_image="python:3.9",
    packages_to_install=["great_expectations==0.15.32", "google-cloud-logging==3.2.5"],
)
def output_expectations_html(is_data_source_added: bool, is_test_suite_created: bool):
    if not is_test_suite_created or not is_data_source_added:
        print("Skipping output_expectations_html")
        return

    import great_expectations as gx
    from great_expectations.render.renderer import ValidationResultsPageRenderer
    from great_expectations.render.view import DefaultJinjaPageView
    from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
    from ruamel import yaml

    # ensure to change GCS to local mount path
    import os

    print(os.getcwd())
    # print(os.listdir('/gcs'))
    print(os.listdir("/gcs/penny-trial-bucket"))

    # Define batch request
    batch_request = BatchRequest(
        datasource_name="my_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name="<YOUR_DATA_ASSET_NAME>",
    )

    batch_request.data_asset_name = "great_expectations_test_csvs/test-data1"

    # Fetch context and add expectation suite
    context = gx.data_context.DataContext(
        context_root_dir="/gcs/penny-trial-bucket/great_expectations"
    )
  
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )
    print(validator.head())

    # Define the checkpoint
    yaml_config = f"""
          name: my_checkpoint
          config_version: 1.0
          class_name: SimpleCheckpoint
          run_name_template: "%Y%m%d-%H%M%S"
          validations:
            - batch_request:
                datasource_name: my_datasource
                data_connector_name: default_inferred_data_connector_name
                data_asset_name: great_expectations_test_csvs/test-data1
                data_connector_query:
                  index: -1
              expectation_suite_name: test_suite
          """

    # Add the checkpoint to context
    context.add_checkpoint(**yaml.load(yaml_config))

    # Run checkpoint to validate the data
    checkpoint_result = context.run_checkpoint(checkpoint_name="my_checkpoint")

    # Validation results are rendered as HTML
    document_model = ValidationResultsPageRenderer().render(
        list(checkpoint_result.run_results.values())[0]["validation_result"]
    )

    # Write validation results as output HTML
    with open("/gcs/penny-trial-bucket/output.html", "w") as writer:
        writer.write(DefaultJinjaPageView().render(document_model))


@dsl.pipeline(
    name="ge-csv",
    description="test ge pipeline with csv files",
    pipeline_root=PIPELINE_ROOT,
)
# You can change the `text` and `emoji_str` parameters here to update the pipeline output
def ge_csv_pipeline(text: str = "Vertex Pipelines"):
    add_datasource_task = add_datasource()
    create_test_suite_task = create_test_suite()
    output_expectations_html(add_datasource_task.output, create_test_suite_task.output)


compiler.Compiler().compile(pipeline_func=ge_csv_pipeline, package_path="ge-csv.json")


api_client = AIPlatformClient(
    project_id=PROJECT_ID,
    region=REGION,
)


response = api_client.create_run_from_job_spec(
    job_spec_path="ge-csv.json",
)