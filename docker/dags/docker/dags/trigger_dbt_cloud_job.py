import os
import time
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


DBT_CLOUD_ACCOUNT_ID = os.getenv("DBT_CLOUD_ACCOUNT_ID")
DBT_CLOUD_JOB_ID = os.getenv("DBT_CLOUD_JOB_ID")
DBT_CLOUD_API_TOKEN = os.getenv("DBT_CLOUD_API_TOKEN")

DBT_CLOUD_BASE_URL = "https://gu425.us1.dbt.com/api/v2"


def trigger_dbt_cloud_job(**context):
    url = f"{DBT_CLOUD_BASE_URL}/accounts/{DBT_CLOUD_ACCOUNT_ID}/jobs/{DBT_CLOUD_JOB_ID}/run/"

    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "cause": "Triggered by Airflow after Snowflake RAW load"
    }

    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()

    run_id = response.json()["data"]["id"]
    print(f"Triggered dbt Cloud job. Run ID: {run_id}")

    context["ti"].xcom_push(key="dbt_cloud_run_id", value=run_id)


def wait_for_dbt_cloud_job(**context):
    run_id = context["ti"].xcom_pull(
        task_ids="trigger_dbt_cloud_job",
        key="dbt_cloud_run_id"
    )

    url = f"{DBT_CLOUD_BASE_URL}/accounts/{DBT_CLOUD_ACCOUNT_ID}/runs/{run_id}/"

    headers = {
        "Authorization": f"Token {DBT_CLOUD_API_TOKEN}",
        "Content-Type": "application/json",
    }

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        run_data = response.json()["data"]
        status = run_data["status"]
        status_humanized = run_data.get("status_humanized")

        print(f"dbt Cloud run {run_id} status: {status_humanized}")

        if status == 10:
            print("dbt Cloud job succeeded.")
            return

        if status in [20, 30]:
            raise Exception(f"dbt Cloud job failed/cancelled. Status: {status_humanized}")

        time.sleep(30)


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="trigger_dbt_cloud_job",
    default_args=default_args,
    description="Trigger dbt Cloud job for SCD2 snapshots and gold marts",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt-cloud", "snowflake", "gold"],
) as dag:

    trigger_job = PythonOperator(
        task_id="trigger_dbt_cloud_job",
        python_callable=trigger_dbt_cloud_job,
    )

    wait_for_job = PythonOperator(
        task_id="wait_for_dbt_cloud_job",
        python_callable=wait_for_dbt_cloud_job,
    )

    trigger_job >> wait_for_job