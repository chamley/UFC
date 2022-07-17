from fileinput import filename
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "seb",
    "email_on_failure": "false",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

catchup = False


def dummy_function():
    file_name = str(datetime.today()) + "_dummy.csv"
    with open(file_name, "w") as f:
        pass


with DAG(
    "ufc-main-dag",
    start_date=datetime(2022, 7, 7),
    schedule_interval="@weekly",
    default_args=default_args,
    catchup=catchup,
) as dag:
    dummy_task = PythonOperator(task_id="dummy_task", python_callable=dummy_function)

    # lambda pulls raw data into S3

    # run tests against structure of raw data

    # lambda runs first transformation

    # run tests great expectations

    # lambda creates temp tables and gets redshift to pull data into first table

    # lambda massages data into final format

    # run tests against great expetations

    # merge into normal data

    # refresh materialized views
