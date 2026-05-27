"""
cms_ingestion_dag.py
Daily DAG: CMS API → S3 → Redshift → dbt models + tests
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago

import sys
sys.path.insert(0, "/opt/airflow/ingestion")
from cms_api_client import CMSApiClient
from s3_uploader import S3Uploader
from schema_validator import SchemaValidator

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-alerts@yourorg.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="cms_claims_daily_ingestion",
    default_args=default_args,
    description="Daily ingestion of CMS Medicare claims into S3 + Redshift, then dbt",
    schedule_interval="0 6 * * *",      # 6 AM UTC daily
    start_date=days_ago(1),
    catchup=True,                        # enables backfilling
    max_active_runs=3,
    tags=["healthcare", "cms", "etl"],
) as dag:

    # ── Task 1: Extract from CMS API ─────────────────────────────────────────
    def extract_cms(**context):
        run_date = context["ds"]          # YYYY-MM-DD from Airflow execution date
        year = run_date[:4]

        client = CMSApiClient()
        validator = SchemaValidator()
        uploader = S3Uploader(bucket="cms-claims-raw", prefix=f"year={year}/")

        for dataset_type in ["part_a", "part_b"]:
            df = client.fetch_all(year=year, dataset=dataset_type)
            validator.validate(df, dataset_type)             # raises on schema drift
            s3_key = uploader.upload(df, dataset_type, run_date)
            print(f"Uploaded {len(df):,} rows → s3://{s3_key}")

    extract = PythonOperator(
        task_id="extract_from_cms_api",
        python_callable=extract_cms,
        provide_context=True,
    )

    # ── Task 2: COPY from S3 into Redshift raw schema ─────────────────────────
    def load_redshift(**context):
        from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
        hook = RedshiftSQLHook(redshift_conn_id="redshift_default")
        run_date = context["ds"]
        year = run_date[:4]

        for table, s3_prefix in [
            ("raw.cms_claims_part_a", f"year={year}/part_a/"),
            ("raw.cms_claims_part_b", f"year={year}/part_b/"),
        ]:
            hook.run(f"""
                COPY {table}
                FROM 's3://cms-claims-raw/{s3_prefix}'
                IAM_ROLE '{{{{ var.value.redshift_iam_role }}}}'
                FORMAT AS PARQUET
                TIMEFORMAT 'auto';
            """)
            print(f"Loaded {table} from s3://cms-claims-raw/{s3_prefix}")

    load = PythonOperator(
        task_id="copy_to_redshift",
        python_callable=load_redshift,
        provide_context=True,
    )

    # ── Task 3: dbt run (staging → intermediate → marts) ─────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run_all_models",
        bash_command="""
            cd /opt/airflow/dbt &&
            dbt run --select staging intermediate marts --target prod
        """,
    )

    # ── Task 4: dbt test ──────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test_all_models",
        bash_command="""
            cd /opt/airflow/dbt &&
            dbt test --target prod
        """,
    )

    # ── Task 5: Data freshness check ──────────────────────────────────────────
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="""
            cd /opt/airflow/dbt &&
            dbt source freshness --target prod
        """,
    )

    # ── DAG dependency chain ──────────────────────────────────────────────────
    extract >> load >> dbt_run >> dbt_test >> dbt_freshness
