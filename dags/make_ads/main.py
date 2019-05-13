
import os
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import make_ads.config as config
import env as env
dag_root = '~/github/mt4-data-pipeline/dags/make_ads'
dag_root = os.path.expanduser(dag_root)

default_args = {
    'owner': 'me',
    'depends_on_past': False,
    'start_date': datetime(2019, 5, 7),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with open(dag_root + '/create_table.sql', 'r') as sql_file:
    query = sql_file.read()


def make_task_name(symbol):
    return "make_table_{}".format(symbol)


def format_query(symbol):
    periods_string = ",".join([str(x) for x in config.periods_to_process])
    return query.format(BQ_PROJECT=env.BQ_PROJECT, SYMBOL_LOWER=symbol.lower(), SYMBOL=symbol, PERIODS=periods_string)


with DAG('forex-data-ads', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:

    starter_task = DummyOperator(task_id='starter')

    for symbol in config.symbols_to_process:
        bq_operator = BigQueryOperator(task_id=make_task_name(symbol),
                                       sql=format_query(symbol),
                                       bigquery_conn_id='google_cloud_default',
                                       write_disposition='WRITE_TRUNCATE',
                                       create_disposition='CREATE_IF_NEEDED',
                                       use_legacy_sql=False)

        starter_task >> bq_operator


