import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import (
    TRADING_PAIRS, RAW_BUCKET, PROCESSED_BUCKET,
    BIGQUERY_DATASET, BIGQUERY_TABLE, KLINE_INTERVAL,
    LOCAL_RAW_DIR, LOCAL_PROCESSED_DIR, AIRFLOW_DEFAULT_ARGS,
    BIGQUERY_WRITE_DISPOSITION, LOOKBACK_DAYS
)

def download_data_for_symbol(symbol, **context):
    output_dir = os.path.join(LOCAL_RAW_DIR, symbol)
    os.makedirs(output_dir, exist_ok=True)
    
    end_date = datetime.utcnow() - timedelta(days=1)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS)
    
    from scripts.binance_utils import download_historical_range, merge_csv_files
    files = download_historical_range(
        symbol=symbol,
        interval=KLINE_INTERVAL,
        start_date=start_date,
        end_date=end_date,
        output_dir=output_dir
    )
    
    if not files:
        raise ValueError(f"No data downloaded for {symbol}")
    
    merged_file = os.path.join(LOCAL_RAW_DIR, f"{symbol}_merged.csv")
    merge_csv_files(files, merged_file)
    
    context['task_instance'].xcom_push(key='raw_file', value=merged_file)
    return merged_file

def process_with_spark(symbol, **context):
    import subprocess
    ti = context['task_instance']
    raw_file = ti.xcom_pull(task_ids=f'process_{symbol}.download_{symbol}', key='raw_file')
    
    processed_dir = os.path.join(LOCAL_PROCESSED_DIR, symbol)
    os.makedirs(processed_dir, exist_ok=True)
    
    spark_script = os.path.join(os.path.dirname(__file__), '..', 'spark', 'transform.py')
    
    cmd = [
        'spark-submit',
        '--master', 'local[*]',
        '--conf', 'spark.driver.memory=1g',
        '--conf', 'spark.executor.memory=1g',
        '--packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0',
        spark_script,
        raw_file,
        processed_dir,
        symbol
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print("STDERR:", result.stderr)
        raise RuntimeError(f"Spark job failed for {symbol}")
    
    parquet_files = list(Path(processed_dir).glob("*.parquet"))
    if not parquet_files:
        raise ValueError(f"No parquet files generated for {symbol}")
    
    processed_file = str(parquet_files[0])
    ti.xcom_push(key='processed_file', value=processed_file)
    return processed_file

with DAG(
    dag_id='cryptolake_etl_pipeline',
    default_args=AIRFLOW_DEFAULT_ARGS,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=2,
    tags=['crypto', 'binance', 'etl', 'gcp'],
) as dag:
    
    setup_dirs = BashOperator(
        task_id='setup_directories',
        bash_command=f'mkdir -p {LOCAL_RAW_DIR} {LOCAL_PROCESSED_DIR}'
    )
    
    for symbol in TRADING_PAIRS:
        with TaskGroup(group_id=f'process_{symbol}') as symbol_group:
            
            download = PythonOperator(
                task_id=f'download_{symbol}',
                python_callable=download_data_for_symbol,
                op_kwargs={'symbol': symbol}
            )
            
            transform = PythonOperator(
                task_id=f'transform_{symbol}',
                python_callable=process_with_spark,
                op_kwargs={'symbol': symbol}
            )
            
            upload_raw = LocalFilesystemToGCSOperator(
                task_id=f'upload_raw_{symbol}',
                src=f'{LOCAL_RAW_DIR}/{symbol}_merged.csv',
                dst=f'raw/{symbol}/{symbol}_{{{{ ds }}}}.csv',
                bucket=RAW_BUCKET,
                gcp_conn_id='google_cloud_default'
            )
            
            upload_processed = LocalFilesystemToGCSOperator(
                task_id=f'upload_processed_{symbol}',
                src="{{ ti.xcom_pull(task_ids='process_" + symbol + ".transform_" + symbol + "', key='processed_file') }}",
                dst=f'processed/{symbol}/{{{{ ds }}}}/{symbol}.parquet',
                bucket=PROCESSED_BUCKET,
                gcp_conn_id='google_cloud_default'
            )
            
            load_to_bq = GCSToBigQueryOperator(
                task_id=f'load_to_bigquery_{symbol}',
                bucket=PROCESSED_BUCKET,
                source_objects=[f'processed/{symbol}/{{{{ ds }}}}/{symbol}.parquet'],
                destination_project_dataset_table=f'{BIGQUERY_DATASET}.{BIGQUERY_TABLE}',
                source_format='PARQUET',
                write_disposition=BIGQUERY_WRITE_DISPOSITION,
                autodetect=True,
                gcp_conn_id='google_cloud_default'
            )
            
            cleanup = BashOperator(
                task_id=f'cleanup_{symbol}',
                bash_command=f'rm -rf {LOCAL_RAW_DIR}/{symbol}* {LOCAL_PROCESSED_DIR}/{symbol}',
                trigger_rule='all_done'
            )
            
            download >> transform >> [upload_raw, upload_processed] >> load_to_bq >> cleanup
        
        setup_dirs >> symbol_group