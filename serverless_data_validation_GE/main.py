
# [START cloudrun_pubsub_server_setup]
# [START run_pubsub_server_setup]
import base64
import os

from flask import Flask, request
from google.cloud import bigquery
from google.oauth2 import service_account


from google.cloud import storage
import ruamel.yaml as yaml
from typing import Any
import logging
import os
from typing import Any, Dict
import ast
from datetime import datetime

import great_expectations as ge
from great_expectations.checkpoint import SimpleCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.profile.json_schema_profiler import JsonSchemaProfiler

import warnings

### Main app
app = Flask(__name__)
    
@app.route("/", methods=["POST"])
def index():
    envelope = request.get_json()
    if not envelope:
        msg = "no Pub/Sub message received"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    if not isinstance(envelope, dict) or "message" not in envelope:
        msg = "invalid Pub/Sub message format"
        print(f"error: {msg}")
        return f"Bad Request: {msg}", 400

    pubsub_message = envelope["message"]

    parameter = "World"
    if isinstance(pubsub_message, dict) and "data" in pubsub_message:
        parameter = base64.b64decode(pubsub_message["data"]).decode("utf-8").strip()

    print(parameter)
    parameter=ast.literal_eval(str(parameter))
    print(parameter)
    
    # try:
    try:
        print("start ge")
        ge=ge_run(parameter)
        print("ge finished")
    except:
        print('something went wrong')
    
    return "", 204
    # finally:
        # return "", 204
    
def ge_run(parameter):
    
    print('process starts')
    print(parameter)
    print(type(parameter))
    parameter=ast.literal_eval(str(parameter))
    print(type(parameter))
    
    # project_id='cio-exegol-lab-3dabae'
    # dataset_id = 'ge_test'
    # bucket_id = 'cio-exegol-lab-3dabae-ge-test'
    # bigquery_dataset = 'ge_test'
    
    project_id=parameter['project_id']
    dataset_id = parameter['dataset_id']
    bucket_id = parameter['bucket_id']
    bigquery_dataset = parameter['bigquery_dataset']
    
    datasource_name = 'ge-test_datasource'
    data_asset_name = 'test-ge'

    connection_str = f"bigquery://{project_id}/{bigquery_dataset}"

    ### Add datasource configuration
    print('adding datasource')
    context = ge.get_context()
    datasource_yaml = f"""
    name: {datasource_name}
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: {connection_str}
    data_connectors:
       default_runtime_data_connector_name:
           class_name: RuntimeDataConnector
           batch_identifiers:
               - default_identifier_name
    """

    context.test_yaml_config(datasource_yaml)
    context.add_datasource(**yaml.safe_load(datasource_yaml))

    ## Add store configurations 
    store_yaml = f"""
    stores:
      expectations_gcs_store:
        class_name: ExpectationsStore
        store_backend:
          class_name: TupleGCSStoreBackend
          project: {project_id}
          bucket: {bucket_id}
          prefix: "expect"
    expectations_store_name: expectations_gcs_store
    """
    store_yaml = yaml.safe_load(store_yaml)
    context.add_store(
        store_name=store_yaml["expectations_store_name"],
        store_config=store_yaml["stores"]["expectations_gcs_store"],
    )

    val_yaml = f"""
    stores:
      validations_gcs_store:
        class_name: ValidationsStore
        store_backend:
          class_name: TupleGCSStoreBackend
          project: {project_id}
          bucket: {bucket_id}
          prefix: "validate"

    validations_store_name: validations_gcs_store
    """

    val_yaml = yaml.safe_load(val_yaml)
    context.add_store(
        store_name=val_yaml["validations_store_name"],
        store_config=val_yaml["stores"]["validations_gcs_store"],
    )

    ### Running Expectation
    print('running expectation')

    batch_request = RuntimeBatchRequest(
        datasource_name=datasource_name,
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name=data_asset_name,
        runtime_parameters={
            "query": parameter['query']},
        batch_identifiers={"default_identifier_name": "default_identifier"},
        batch_spec_passthrough={
            "bigquery_temp_table": "ge_temp"
        },
    )
    
    ### Expectations    
    expectation_suite_name = "ge-test-suite" + "_" + str(datetime.now())
    # context.create_expectation_suite(
        # expectation_suite_name=expectation_suite_name,
        # overwrite_existing=True
    # )

    simple_schema= {
            "$id": "https://example.com/address.schema.json",
            # "$schema": "http://json-schema.org/draft-07/schema#",
            "$schema": "/schema",
            "type": "object",
            "properties": parameter['properties']
            # "properties": {
                # "pickup_location_id": {"type": "integer"},
                # "vendor_id": {"type": "integer"},
                # "store_and_fwd_flag": {"type": "boolean"},
                # "vendor_id": {"enum": [1,2,4]},
                # "passenger_count": {"type": "integer", "minimum": 0, "maximum": 130}
            # }
        }
        
    print("Generating suite...")
    profiler = JsonSchemaProfiler()
    suite = profiler.profile(simple_schema, expectation_suite_name)
    context.save_expectation_suite(suite)


    batch = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=expectation_suite_name
    )
    
    # Expectations
    # batch.expect_column_values_to_not_be_null(column="first_name")
    
    # batch.save_expectation_suite(discard_failed_expectations=False)

    ### Run Validation
    
    print("running validation")
    
    checkpoint_name = 'test-check'
    checkpoint_config = {
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "run_name_template": f"%Y%m%d-%H%M%S-{checkpoint_name}",
        "validations": [
            {
                "batch_request": batch_request.to_json_dict(),
                "expectation_suite_name": expectation_suite_name
            },
        ],
    }

    checkpoint = SimpleCheckpoint(
        name=checkpoint_name,
        data_context=context,
        **checkpoint_config
    )
    
    checkpoint_result = checkpoint.run()
    
    return '', 200
    
if __name__ == "__main__":
    PORT = int(os.getenv("PORT")) if os.getenv("PORT") else 8080

    # This is used when running locally. Gunicorn is used to run the
    # application on Cloud Run. See entrypoint in Dockerfile.
    app.run(host="127.0.0.1", port=PORT, debug=True)
