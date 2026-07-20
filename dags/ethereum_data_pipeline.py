import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as SnowflakeOperator

# ==============================================================================
# CONFIGURATION & EDGE-CASE HARDENING
# ==============================================================================
# 1. Double-check that your physical folder is spelled "Ethereum Project"
DBT_PROJECT_DIR = r"C:\Ethereum Project\ETH_DBT"

# 2. Pointing directly to the dbt executable avoids Windows shell activation bugs
DBT_EXECUTABLE = r"C:\Ethereum Project\.venv\Scripts\dbt.exe"

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ethereum_medallion_pipeline',
    default_args=default_args,
    description='Orchestrates Bronze ingestion via Snowflake and Silver/Gold transformations via dbt',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,  # Full Refresh strategy: Only run the current day, no backfilling history
) as dag:

    # ==========================================
    # STAGE 1: BRONZE LAYER (RAW INGESTION)
    # ==========================================
    # Airflow instructs Snowflake to pull the latest files from S3 into your raw tables
    load_bronze_tables = SnowflakeOperator(
        task_id='load_bronze_tables',
        conn_id='snowflake_default',
        sql='''
            SET CURRENT_MONTH_PATTERN = (Select CONCAT('.*date=', TO_VARCHAR(CURRENT_DATE() - 3, 'YYYY-MM'), '.*'));

            COPY INTO ETH.ETH_SCHEMA.CONTRACTS
            FROM (
                SELECT t.$1:address, t.$1:block_hash, t.$1:block_number, t.$1:block_timestamp, t.$1:bytecode, t.$1:date, t.$1:last_modified
                FROM @ETH.ETH_SCHEMA.CONTRACTS_STAGE t
            ) PATTERN = $CURRENT_MONTH_PATTERN;

            COPY INTO ETH.ETH_SCHEMA.TOKEN_TRANSFERS
            FROM (
                SELECT t.$1:block_hash, t.$1:block_number, t.$1:block_timestamp, t.$1:date, t.$1:from_address, t.$1:to_address, t.$1:last_modified, t.$1:log_index, t.$1:token_address, t.$1:transaction_hash, t.$1:value
                FROM @ETH.ETH_SCHEMA.TOKEN_TRANSFERS t
            ) PATTERN = $CURRENT_MONTH_PATTERN;

            COPY INTO ETH.ETH_SCHEMA.TRANSACTIONS
            FROM (
                SELECT t.$1:block_hash, t.$1:block_number, t.$1:block_timestamp, t.$1:date, t.$1:from_address, t.$1:gas, t.$1:gas_price, t.$1:hash, t.$1:input, t.$1:last_modified, t.$1:max_fee_per_gas, t.$1:max_priority_fee_per_gas, t.$1:nonce, t.$1:receipt_contract_address, t.$1:receipt_cumulative_gas_used, t.$1:receipt_effective_gas_price, t.$1:receipt_gas_used, t.$1:receipt_status, t.$1:to_address, t.$1:transaction_index, t.$1:transaction_type, t.$1:value
                FROM @ETH.ETH_SCHEMA.TRANSACTIONS t
            ) PATTERN = $CURRENT_MONTH_PATTERN;
        ''',
    )

    # ==========================================
    # STAGE 2: DATA FRESHNESS TRIPWIRE
    # ==========================================
    # Validates that your raw tables actually received new data today before transforming
    # Uses 'cd /d' to guarantee Windows changes disk drive partitions if necessary
    dbt_source_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=f'cd /opt/airflow/dags/ETH_DBT && dbt source freshness',
    )

    # ==========================================
    # STAGE 3: SILVER & GOLD LAYER BUILDER
    # ==========================================
    # Tells dbt to read your local models/ folders and recreate clean tables in Snowflake
    run_dbt_pipeline = BashOperator(
        task_id='run_dbt_pipeline',
        bash_command=f'cd /opt/airflow/dags/ETH_DBT && dbt run',
    )

    # ==========================================
    # STAGE 4: DATA QUALITY TESTING
    # ==========================================
    # Runs the testing suite (e.g., checking for unique IDs or null values)
    test_dbt_pipeline = BashOperator(
        task_id='test_dbt_pipeline',
        bash_command='cd /opt/airflow/dags/ETH_DBT && dbt test',
    )

    # ==========================================
    # PIPELINE DEPENDENCY GRAPH
    # ==========================================
    # Load raw data -> Verify it's fresh -> Build downstream layers -> Test the final results
    load_bronze_tables >> dbt_source_freshness >> run_dbt_pipeline >> test_dbt_pipeline
