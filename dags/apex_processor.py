import ast
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator, ShortCircuitOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from google.cloud import storage

#
# Setup our DAG
#
APEX_SOURCE_BUCKET = "qbank-test-bucket"
APEX_ROOT_DIR = 'import/'
APEX_PROCESSED_DIR = 'processed/'
APEX_ERRORED_DIR = 'errored/'
DATASET_NAME="testdataset"
TABLE_NAME="CorrespondentOffice"
PII_PRESENT = False

def list_gcs_files_by_regex(pattern: str):
    client = storage.Client()
    result = []
    for blob in client.list_blobs(APEX_SOURCE_BUCKET, prefix="import"):
        print(f"Comparing {pattern} with file {blob.name}")
        if pattern in blob.name.lower():
            result.append(blob.name)

    return result

def check_file_list(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    result = True

    if len(file_list) == 0:
        print("No files to process")
        result = False

    return result

def perform_bigquery_load(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    op = GCSToBigQueryOperator(
            task_id='WrappedPopulateBigQuery',
            bucket=APEX_SOURCE_BUCKET,
            source_objects=file_list,
            destination_project_dataset_table = "{}.{}".format(DATASET_NAME, TABLE_NAME),
            create_disposition='CREATE_IF_NEEDED',
            write_disposition='WRITE_APPEND',
            source_format='AVRO',
            max_bad_records=0)

    op.execute(context)

def check_redact_condition(**context):
    return ["Redact", "LoadBigQuery"] if PII_PRESENT else ["LoadBigQuery"]

def create_dag(dag_id, schedule, filepattern, default_args):
    def ingest_file(**context):
        files = list_gcs_files_by_regex(filepattern)
        context["ti"].xcom_push(key="return_value", value=files)

    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args = default_args)

    with dag:
        list_gcs_files = PythonOperator(
                task_id='ListGCSFiles',
                pattern=filepattern,
                provide_context=True,
                python_callable=ingest_file)

        check_files = ShortCircuitOperator(
                task_id='CheckFileList',
                provide_context=True,
                python_callable=check_file_list)

        archive = DummyOperator(
                    task_id='Archive')

        check_redact = BranchPythonOperator(
                task_id="RedactCondition",
                python_callable=check_redact_condition,
                provide_context=True)

        redact = DummyOperator(
                task_id='Redact')

        load_bigquery = PythonOperator(
                task_id='LoadBigQuery',
                provide_context=True,
                trigger_rule=TriggerRule.ONE_SUCCESS,
                python_callable=perform_bigquery_load)

        end = DummyOperator(
                task_id='End')

        list_gcs_files >> check_files >> archive
        archive >> check_redact
        check_redact >> redact >> load_bigquery >> end
        check_redact >> load_bigquery >> end

    return dag

# Setup DAG
dag_name = TABLE_NAME.lower()
dag_id = f"ingest_table_{dag_name}"
schedule = '@daily'

default_args = {'owner': 'apex',
                'schedule_interval': None,
		'start_date': days_ago(1)
	       }

dag = create_dag(dag_id, schedule, dag_name, default_args)
