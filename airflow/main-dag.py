from fileinput import filename
from airflow import DAG
from datetime import datetime, timedelta


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
    pass
