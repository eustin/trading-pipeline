
import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_operator import GoogleCloudStorageHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.python_operator import PythonOperator

import env

csv_file_dir = os.path.expanduser(env.CSV_FILE_DIR)

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 6),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

raw_schema = [
      {
        "mode": "NULLABLE",
        "name": "symbol",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "period",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "time",
        "type": "TIMESTAMP"
      },
      {
        "mode": "NULLABLE",
        "name": "open",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "high",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "low",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "close",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "tick_volume",
        "type": "INTEGER"
      }
    ]


def list_local_files():
    full_paths = [os.path.join(csv_file_dir, x) for x in os.listdir(csv_file_dir)]
    names_only = os.listdir(csv_file_dir)
    return full_paths, names_only


def upload_files(**context):
    full_paths, names_only = list_local_files()
    for i in range(len(full_paths)):
        move_to_gcs = FileToGoogleCloudStorageOperator(task_id='to_gcs', src=full_paths[i], dst=names_only[i],
                                                       bucket=env.GCS_BUCKET)
        logging.info('uploading file ' + names_only[i])
        move_to_gcs.execute(context)
        os.remove(full_paths[i])


def get_bucket_file_names(gcs_hook):
    return gcs_hook.list(bucket=env.GCS_BUCKET)


def move_from_gcs_to_bq(**context):
    gcs_hook = GoogleCloudStorageHook()
    file_list = get_bucket_file_names(gcs_hook)
    destination_table = '{}.raw.raw'.format(env.BQ_PROJECT)

    for file in file_list:
        gcs_to_bq_operator = GoogleCloudStorageToBigQueryOperator(task_id='temp', bucket=env.GCS_BUCKET,
                                                                  source_objects=[file],
                                                                  destination_project_dataset_table=destination_table,
                                                                  write_disposition='WRITE_APPEND',
                                                                  schema_fields=raw_schema, skip_leading_rows=1)
        gcs_to_bq_operator.execute(context)
        gcs_hook.delete(bucket=env.GCS_BUCKET, object=file)


with DAG('forex-data-pipeline', default_args=default_args, schedule_interval=timedelta(hours=1)) as dag:

    move_files = PythonOperator(task_id='move-files', python_callable=upload_files, provide_context=True)

    move_to_bigquery = PythonOperator(task_id='move-to-bigquery', python_callable=move_from_gcs_to_bq,
                                       provide_context=True)

    move_files >> move_to_bigquery