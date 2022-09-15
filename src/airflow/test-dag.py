from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

default_args = {
    "owner": "seb",
    "email_on_failure": "false",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "depends_on_past": False,
}


def print_todays_date(ds, **kwargs):
    logging.info("ohai from print_todays_date")
    logging.info(f"Todays date is {ds}, have a nice day!")


def print_a_happy_message(ds, **kwargs):
    logging.info("ohai from print_a_happy_message")
    logging.info("this is another task. hooray")


with DAG(
    "ufc-test-dag",
    default_args=default_args,
    schedule_interval="@weekly",
    catchup=True,
    start_date=datetime(2022, 7, 20),
) as dag:
    logging.info("ohai from the dag")
    date_task = PythonOperator(
        task_id="date_task",
        python_callable=print_todays_date,
        provide_context=True,
        dag=dag,
    )
    happiness_task = PythonOperator(
        task_id="happiness_task",
        python_callable=print_a_happy_message,
        provide_context=True,
        dag=dag,
    )


date_task >> happiness_task
