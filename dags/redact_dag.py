import uuid

from airflow import DAG, AirflowException
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator, ShortCircuitOperator

from googleapiclient.discovery import build
from google.cloud import storage

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
TABLE_NAME = "AccountMaster"
REDACT_FIELDS = "Address1,Address2"
PII_PRESENT = True
BATCH_SIZE = 250

FLEX_TEMPLATE_PATH = "gs://qbank-test-bucket/dataflow_dlp/template_metadata"


def list_gcs_files_by_regex(pattern: str):
    result = []
    for blob in client.list_blobs(APEX_SOURCE_BUCKET, prefix=APEX_ROOT_DIR):
        print(f"Comparing {pattern} with file {blob.name}")
        if pattern in blob.name.lower():
            result.append(blob.name)
    print(result)

    return result

def check_file_list(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")

    print(file_list)

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
        new_f = file.replace(old_dir, f"dlp_input/{TABLE_NAME}")

        # source_bucket.copy_blob(
        #     source_blob, destination_bucket, new_f)

        # keep originals. If redact is successful, we will overwrite
        # source_blob.delete()

def delete_dlp_files(**context):
    ti = context["ti"]
    file_list = ti.xcom_pull(task_ids="ListGCSFiles")
    bucket = client.get_bucket(APEX_SOURCE_BUCKET)

    input_files = []
    output_files = []


    dlp_input = bucket.list_blobs(prefix=f"dlp_input/{TABLE_NAME}/")
    for blob in dlp_input:
        input_files.append(blob.name)
        # blob.delete()
    # bucket.delete_blobs(input_files)


    dlp_output = bucket.list_blobs(prefix=f"dlp_output/{TABLE_NAME}/")
    for blob in dlp_output:
        output_files.append(blob.name)
        # blob.delete()
    # bucket.delete_blobs(output_files)

    print("input files")
    print(input_files)
    print("output files")
    print(output_files)

def perform_redact(**context):
    input = f"gs://{APEX_SOURCE_BUCKET}/dlp_input/{TABLE_NAME}/*"
    output = f"gs://{APEX_SOURCE_BUCKET}/dlp_output/{TABLE_NAME}/{TABLE_NAME.lower()}{uuid.uuid4()}"
    jobname = f"apexredact{TABLE_NAME.lower()}{uuid.uuid4().hex}"

    print(f"Input files location: {input}")
    print(f"Output files location: {output}")
    print(f"Dataflow job name: {jobname}")

    parameters = {
        "input": input,
        "output": output,
        "redact_fields": REDACT_FIELDS,
        "setup_file": "/dataflow/template/setup.py",
        "table_name": TABLE_NAME,
        "batch_size": BATCH_SIZE
    }

    dataflow = build('dataflow', 'v1b3')
    flex_request = dataflow.projects().locations().flexTemplates().launch(
        projectId=APEX_PROJECT,
        location='us-central1',
        body={
            'launchParameter': {
                'jobName': jobname,
                'containerSpecGcsPath': FLEX_TEMPLATE_PATH,
                'parameters': parameters
            }
        }
    )
    response = flex_request.execute()
    job_id = response['job']['id']
    current_state = 'JOB_STATE_QUEUED'

    while current_state != 'JOB_STATE_DONE':
        dataflow_api_request = dataflow.projects().locations().jobs().get(
            projectId=APEX_PROJECT,
            location='us-central1',
            jobId=job_id
        )

        get_response = dataflow_api_request.execute()
        current_state = get_response['currentState']

        if current_state == "JOB_STATE_FAILED":
            raise AirflowException(f"Dataflow job failed. Check logs for job_id {job_id}")

    return current_state

def write_to_bq_redacted(**context):
    file_list = []
    table = f"{APEX_PROJECT}.{DATASET_NAME}.{TABLE_NAME}_redacted"

    for blob in client.list_blobs(APEX_SOURCE_BUCKET, prefix=f"dlp_output/{TABLE_NAME}"):
        file_list.append(blob.name)

    op = GoogleCloudStorageToBigQueryOperator(
        task_id="RedactedAvroToBigQuery",
        source_objects=file_list,
        bucket=APEX_SOURCE_BUCKET,
        destination_project_dataset_table=table,
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
        destination_project_dataset_table=f"{APEX_PROJECT}.{DATASET_NAME}.{TABLE_NAME}_non_redacted",
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

        move_files_to_be_redacted = PythonOperator(
            task_id='MovePiiFiles',
            provide_context=True,
            python_callable=move_files)

        redact = PythonOperator(
            task_id='DLPRedactPii',
            provide_context=True,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=perform_redact
        )

        load_bigquery_redacted = PythonOperator(
            task_id='LoadBigQueryRedacted',
            provide_context=True,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=write_to_bq_redacted)

        load_bigquery_non_redacted = PythonOperator(
            task_id='LoadBigQueryNonRedacted',
            provide_context=True,
            trigger_rule=TriggerRule.ONE_SUCCESS,
            python_callable=write_to_bq_non_redacted)

        delete_files = PythonOperator(
            task_id='DeleteRedactedFiles',
            provide_context=True,
            python_callable=delete_dlp_files)

        end = DummyOperator(
            task_id='End')

        list_gcs_files >> check_files >> archive >> check_redact
        # redact
        check_redact >> move_files_to_be_redacted >> redact >> load_bigquery_redacted >> delete_files >> end
        # TODO move redacted file back into the import_testing_airflow directory
        # do not redact
        check_redact >> load_bigquery_non_redacted >> end

    return dag


# Setup DAG
dag_name = TABLE_NAME.lower()
dag_id = f"ingest_table_update_{dag_name}"
schedule = '@daily'

default_args = {'owner': 'apex',
                'schedule_interval': None,
                'start_date': days_ago(1)
                }

dag = create_dag(dag_id, schedule, dag_name, default_args)
