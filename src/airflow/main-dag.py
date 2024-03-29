import json
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.python import PythonOperator
import boto3
import logging

default_args = {
    "owner": "seb",
    "email_on_failure": "false",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # "start_date": datetime(2022, 7, 20),
}


def dummy_function(ds, **kwargs):
    print(ds)
    x = 1


def trigger_extractor_lambda(ds, **kwargs):
    logging.info(ds)
    logging.info(date.fromisoformat(ds))
    lambdaclient = boto3.client("lambda", "us-east-1")
    payload = {
        "dates": {
            "start": f"{date.fromisoformat(ds)-timedelta(weeks=1)}",
            "end": f"{date.fromisoformat(ds)}",
        }
    }
    lambdaclient.invoke(
        FunctionName="ufc-extractor",
        InvocationType="Event",
        Payload=json.dumps(payload),
    )


def trigger_t1_lambda(ds, **kwargs):
    lambdaclient = boto3.client("lambda", "us-east-1")
    payload = {
        "dates": {
            "start": f"{date.fromisoformat(ds)-timedelta(weeks=1)}",
            "end": f"{date.fromisoformat(ds)}",
        }
    }
    lambdaclient.invoke(
        FunctionName="ufc-t1",
        InvocationType="Event",
        Payload=json.dumps(payload),
    )


with DAG(
    "ufc-main-dag",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=True,
    start_date=datetime(1994, 1, 1),
) as dag:
    dummy_task = PythonOperator(
        task_id="dummy_task",
        python_callable=dummy_function,
        dag=dag,
        provide_context=True,
    )
    logging.info("ohai")
    # lambda pulls raw data into S3
    extractor_task = PythonOperator(
        task_id="extractor_task",
        python_callable=trigger_extractor_lambda,
        provide_context=True,
        dag=dag,
    )

    t1_task = PythonOperator(
        task_id="t1_task",
        python_callable=trigger_t1_lambda,
        provide_context=True,
        dag=dag,
    )

    # run tests against structure of raw data

    # lambda runs first transformation

    # run tests great expectations

    # lambda creates temp tables and gets redshift to pull data into first table

    # lambda massages data into final format

    # run tests against great expetations

    # merge into normal data

    # refresh materialized views

dummy_task >> extractor_task >> t1_task
