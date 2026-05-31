import os
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

load_dotenv()

# ============================================================
# AWS S3 CONFIG
# ============================================================
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# ============================================================
# SNOWFLAKE CONFIG
# ============================================================
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# ============================================================
# TABLES TO LOAD
# ============================================================
TABLES = ["customers", "accounts", "transactions"]


# ============================================================
# SNOWFLAKE CONNECTION
# ============================================================
def get_snowflake_connection():
    """
    Create Snowflake connection.
    """
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )


# ============================================================
# SETUP SNOWFLAKE OBJECTS
# ============================================================
def setup_snowflake_objects():
    """
    Creates:
    - Database
    - Bronze schema
    - Parquet file format
    - S3 external stage
    - Bronze tables (raw VARIANT format)
    """

    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Ensure correct context
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    # --------------------------------------------------------
    # File format for parquet files
    # --------------------------------------------------------
    cur.execute("""
        CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
        TYPE = PARQUET
    """)

    # --------------------------------------------------------
    # External stage pointing to AWS S3 raw layer
    # --------------------------------------------------------
    cur.execute(f"""
        CREATE OR REPLACE STAGE S3_RAW_STAGE
        URL = 's3://{S3_BUCKET}/raw/'
        CREDENTIALS = (
            AWS_KEY_ID = '{AWS_ACCESS_KEY_ID}'
            AWS_SECRET_KEY = '{AWS_SECRET_ACCESS_KEY}'
        )
        FILE_FORMAT = PARQUET_FORMAT
    """)

    # --------------------------------------------------------
    # Bronze tables
    # One raw VARIANT column
    # --------------------------------------------------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            raw_data VARIANT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            raw_data VARIANT
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            raw_data VARIANT
        )
    """)

    print("Snowflake setup completed.")

    cur.close()
    conn.close()


# ============================================================
# GENERATE COPY INTO SQL
# ============================================================
def get_copy_sql(table_name):
    """
    Load parquet row into raw_data VARIANT column.

    Example:

    S3 parquet record:
    {
        "id": 1,
        "customer_id": 20,
        "balance": 1000
    }

    gets loaded as-is into:

    raw_data VARIANT
    """

    return f"""
        COPY INTO {table_name} (raw_data)
        FROM (
            SELECT $1
            FROM @S3_RAW_STAGE/{table_name}/
        )
        FILE_FORMAT = (TYPE = PARQUET)
        PATTERN = '.*\\.parquet'
        ON_ERROR = 'CONTINUE'
    """


# ============================================================
# LOAD TABLE FROM S3 → SNOWFLAKE BRONZE
# ============================================================
def load_table_to_bronze(table_name):

    conn = get_snowflake_connection()
    cur = conn.cursor()

    # Ensure correct context
    cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
    cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")

    copy_sql = get_copy_sql(table_name)

    cur.execute(copy_sql)

    print(f"Loaded data into Snowflake Bronze: {table_name}")

    cur.close()
    conn.close()


# ============================================================
# AIRFLOW DAG CONFIG
# ============================================================
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="s3_to_snowflake_bronze",
    default_args=default_args,
    description="Load S3 parquet files into Snowflake Bronze layer",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # --------------------------------------------------------
    # Setup Snowflake objects
    # --------------------------------------------------------
    setup_task = PythonOperator(
        task_id="setup_snowflake_objects",
        python_callable=setup_snowflake_objects,
    )

    # --------------------------------------------------------
    # Load each table
    # --------------------------------------------------------
    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=load_table_to_bronze,
        op_kwargs={"table_name": "customers"},
    )

    load_accounts = PythonOperator(
        task_id="load_accounts",
        python_callable=load_table_to_bronze,
        op_kwargs={"table_name": "accounts"},
    )

    load_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=load_table_to_bronze,
        op_kwargs={"table_name": "transactions"},
    )

    trigger_dbt_cloud = TriggerDagRunOperator(
    task_id="trigger_dbt_cloud_job",
    trigger_dag_id="trigger_dbt_cloud_job",
    wait_for_completion=False,
    )
    
    # --------------------------------------------------------
    # DAG dependency
    # --------------------------------------------------------
    setup_task >> [load_customers, load_accounts, load_transactions] >> trigger_dbt_cloud