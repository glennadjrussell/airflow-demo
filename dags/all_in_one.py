from airflow import models
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
#from apex.airflow.operators import GCSFindFileByPatternOperator
import google.cloud.storage as storage

TEST = "test"


default_args = {}

def list_files(**kwargs):
    print("Entering list_files")
    test_bucket = "qbank-test-bucket"
    gcs = storage.Client()

    try:
        blobs = gcs.list_blobs(test_bucket)
        for blob in blobs:
            print(blob.name)
    except:
        print(sys.exc_info()[0])

    return True

dag = models.DAG(
        dag_id = f"ingest_{TEST}",
        default_args=default_args,
        start_date=days_ago(2),
        schedule_interval=None,
        tags=["BigQuery", ""]
)

def check_files(**kwargs):
    pass

# Configure tasks
find_files = DummyOperator(
        task_id='find_files_by_pattern',
        dag=dag
)

find_gcs_files = PythonOperator(
        task_id='list_gcs_files',
        python_callable=list_files,
        provide_context=True,
        dag=dag
)

file_filter = ShortCircuitOperator(
        task_id='check_file_list',
        python_callable=lambda: True,
        dag=dag
)
move_files = DummyOperator(

        task_id='move_files',
        dag=dag
)


redact_pii = DummyOperator(
        task_id='redact_pii',
        dag=dag
)


load_bigquery = DummyOperator(
        task_id='load_bigquery',
        dag=dag
)


notify_success = DummyOperator(
        task_id='notify_success',
        dag=dag
)


notify_failure = DummyOperator(
        task_id='notify_failure',
        dag=dag
)


find_gcs_files >> file_filter >> find_files >> move_files >> redact_pii >> load_bigquery >> [notify_success, notify_failure]

