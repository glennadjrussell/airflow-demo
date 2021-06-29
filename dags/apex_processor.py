from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator, ShortCircuitOperator

from google.cloud import storage
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/jhanrattyqarik.com/apex/beam-dlp-pipeline/qbank_keys.json"

client = storage.Client()
#
# Setup our DAG
#
APEX_PROJECT = 'qbank-266411'
APEX_SOURCE_BUCKET = "qbank-test-bucket"
APEX_ROOT_DIR = 'import_testing_airflow/'
APEX_PROCESSED_DIR = 'processed/'
APEX_ERRORED_DIR = 'errored/'
DATASET_NAME = "testdataset"
TABLE_NAME = "CorrespondentOffice"
PII_PRESENT = True

DLP_OUTPUT = f"dlp_output/{TABLE_NAME}/"


def list_gcs_files_by_regex(pattern: str):
    result = []
    for blob in client.list_blobs(APEX_SOURCE_BUCKET, prefix="import"):
        print(f"Comparing {pattern} with file {blob.name}")
        if pattern in blob.name.lower():
            result.append(blob.name)
    print(result)

    return result

def check_file_list(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    result = True

    if len(file_list) == 0:
        print("No files to process")
        result = False

    return result

def move_files(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    source_bucket = client.get_bucket(APEX_SOURCE_BUCKET)
    destination_bucket = client.get_bucket(APEX_SOURCE_BUCKET)

    print(file_list)

    for file in file_list:
        source_blob = source_bucket.blob(file)

        old_dir = file.split("/")[0]
        new_f = file.replace(old_dir, "dlp_input")

        source_bucket.copy_blob(
            source_blob, destination_bucket, new_f)

def delete_dlp_files(**context):
    bucket = client.get_bucket(APEX_SOURCE_BUCKET)

    dlp_input = bucket.list_blobs(prefix="dlp_input")
    for blob in dlp_input:
        print(blob.name)
        blob.delete()

    dlp_output = bucket.list_blobs(prefix="dlp_output")
    for blob in dlp_output:
        print(blob.name)
        blob.delete()

def perform_redact(**context):
    files = []
    redact_success = []

    ### This task also writes the output to BigQuery and Google Cloud Storage

    op = DataflowStartFlexTemplateOperator(
        task_id="DataflowRedactPii",
        project_id=APEX_PROJECT,
        do_xcom_push=True,
        location="us-central1",
        body={
            "launchParameter": {
                "containerSpecGcsPath": "gs://qbank-test-bucket/dataflow_dlp/template_metadata",
                "jobName": "apexredact3",
                "parameters": {
                    "input": f"gs://{APEX_SOURCE_BUCKET}/dlp_input/*",
                    "output": f"gs://{APEX_SOURCE_BUCKET}/{DLP_OUTPUT}",
                    "redact_fields": "Address1",
                    "setup_file": "/dataflow/template/setup.py",
                    "table_name": TABLE_NAME
                },
            }
        }
    )
    print(f"Redact complete for file {str(files)}")
    # redact_success.append(file_name)
    # location: "us-east1"

    op.execute(context)

    return redact_success


def write_to_bq_redacted(**context):
    file_list = []

    for blob in client.list_blobs(APEX_SOURCE_BUCKET, prefix="dlp_output/"):
        file_list.append(blob.name)


    print(file_list)

    op = GoogleCloudStorageToBigQueryOperator(
        task_id="RedactedAvroToBigQuery",
        bigquery_conn_id='bigquery',
        source_objects=file_list,
        bucket=APEX_SOURCE_BUCKET,
        destination_project_dataset_table=f"{APEX_PROJECT}.{DATASET_NAME}.jen_test_redact",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        source_format='AVRO',
    )

    op.execute(context)

def write_to_bq_non_redacted(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    op = GoogleCloudStorageToBigQueryOperator(
        task_id="NonRedactedAvroToBigQuery",
        source_objects=file_list,
        bucket=APEX_SOURCE_BUCKET,
        destination_project_dataset_table=f"{APEX_PROJECT}.{DATASET_NAME}.jen_test_redact_4",
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        source_format='AVRO',
    )

    op.execute(context)


def check_redact_condition(**context):
    return ["MovePiiFiles"] if PII_PRESENT else ["LoadBigQueryNonRedacted"]

def create_dag(dag_id, schedule, filepattern, default_args):
    def ingest_file(**context):
        files = list_gcs_files_by_regex(filepattern)
        context["ti"].xcom_push(key="return_value", value=files)

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        # list_gcs_files = PythonOperator(
        #     task_id='ListGCSFiles',
        #     pattern=filepattern,
        #     provide_context=True,
        #     python_callable=ingest_file)
        #
        # check_files = ShortCircuitOperator(
        #     task_id='CheckFileList',
        #     provide_context=True,
        #     python_callable=check_file_list)
        #
        # archive = DummyOperator(
        #     task_id='Archive')
        #
        # check_redact = BranchPythonOperator(
        #     task_id="RedactCondition",
        #     python_callable=check_redact_condition,
        #     provide_context=True)
        #
        # # move_files_to_be_redacted = PythonOperator(
        # #     task_id='MovePiiFiles',
        # #     provide_context=True,
        # #     python_callable=move_files)

        redact = PythonOperator(
            task_id='DataflowRedactPii',
            provide_context=True,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=perform_redact
        )

        # load_bigquery_redacted = PythonOperator(
        #     task_id='LoadBigQueryRedacted',
        #     provide_context=True,
        #     trigger_rule=TriggerRule.ONE_SUCCESS,
        #     python_callable=write_to_bq_redacted)
        #
        # load_bigquery_non_redacted = PythonOperator(
        #     task_id='LoadBigQueryNonRedacted',
        #     provide_context=True,
        #     trigger_rule=TriggerRule.ONE_SUCCESS,
        #     python_callable=write_to_bq_non_redacted)
        #
        # delete_files = PythonOperator(
        #     task_id='DeleteRedactedFiles',
        #     provide_context=True,
        #     python_callable=delete_dlp_files)
        #
        # end = DummyOperator(
        #     task_id='End')

        # list_gcs_files >> check_files >> archive >> check_redact
        # # redact
        # check_redact >> move_files_to_be_redacted >> \

        redact

        # >> delete_files >> end
        # do not redact
        # check_redact >> load_bigquery_non_redacted >> end

    return dag


# Setup DAG
dag_name = TABLE_NAME.lower()
dag_id = f"ingest_table_jen_test_{dag_name}"
schedule = '@daily'

default_args = {'owner': 'apex',
                'schedule_interval': None,
                'start_date': days_ago(1)
                }

dag = create_dag(dag_id, schedule, dag_name, default_args)
