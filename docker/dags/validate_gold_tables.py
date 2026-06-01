import os
import sys
import snowflake.connector
from pathlib import Path
from dotenv import load_dotenv

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_GOLD_SCHEMA")

required_vars = {
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
}

missing = [k for k, v in required_vars.items() if not v]

if missing:
    raise ValueError(f"Missing environment variables: {missing}")


def run_check(cur, check_name, sql):
    print(f"Running check: {check_name}")

    cur.execute(sql)
    result = cur.fetchone()[0]

    if result > 0:
        raise Exception(f"FAILED: {check_name}. Failure count: {result}")

    print(f"PASSED: {check_name}")


def main():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

    cur = conn.cursor()

    try:
        checks = [
            (
                "dim_customers.customer_id not null",
                """
                select count(*)
                from dim_customers
                where customer_id is null
                """,
            ),
            (
                "dim_customers.email not null",
                """
                select count(*)
                from dim_customers
                where email is null
                """,
            ),
            (
                "dim_customers.is_current not null",
                """
                select count(*)
                from dim_customers
                where is_current is null
                """,
            ),
            (
                "dim_accounts.account_id not null",
                """
                select count(*)
                from dim_accounts
                where account_id is null
                """,
            ),
            (
                "dim_accounts.account_type valid values",
                """
                select count(*)
                from dim_accounts
                where account_type not in ('CHECKING', 'SAVINGS')
                   or account_type is null
                """,
            ),
            (
                "fct_transactions.transaction_id not null",
                """
                select count(*)
                from fct_transactions
                where transaction_id is null
                """,
            ),
            (
                "fct_transactions.amount not null",
                """
                select count(*)
                from fct_transactions
                where amount is null
                """,
            ),
            (
                "fct_transactions.amount non-negative",
                """
                select count(*)
                from fct_transactions
                where amount < 0
                """,
            ),
            (
                "fct_transactions.status valid",
                """
                select count(*)
                from fct_transactions
                where status != 'COMPLETED'
                   or status is null
                """,
            ),
        ]

        for check_name, sql in checks:
            run_check(cur, check_name, sql)

        print("All GOLD data quality checks passed.")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
        sys.exit(1)