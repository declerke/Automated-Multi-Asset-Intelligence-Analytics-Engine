import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from config.config import RAW_BUCKET, LOCAL_RAW_DIR, KLINE_INTERVAL
from scripts.binance_utils import download_binance_bulk_zip


def download_single_test(**context):
    symbol = "BTCUSDT"
    test_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    output_dir = os.path.join(LOCAL_RAW_DIR, 'test')
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Testing download for {symbol} on {test_date}")
    
    csv_path = download_binance_bulk_zip(
        symbol=symbol,
        interval=KLINE_INTERVAL,
        date=test_date,
        output_dir=output_dir
    )
    
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, 'r') as f:
            lines = f.readlines()
            print(f"Downloaded file has {len(lines)} rows")
            print(f"First row: {lines[0]}")
        
        context['task_instance'].xcom_push(key='test_file', value=csv_path)
        return csv_path
    else:
        raise ValueError("Failed to download test file")


with DAG(
    dag_id='test_single_symbol_download',
    default_args={
        'owner': 'cryptolake',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    description='Test downloading a single symbol from Binance',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'binance'],
) as dag:
    
    setup = BashOperator(
        task_id='setup_test_dir',
        bash_command=f'mkdir -p {LOCAL_RAW_DIR}/test'
    )
    
    download = PythonOperator(
        task_id='download_test_data',
        python_callable=download_single_test,
        provide_context=True
    )
    
    cleanup = BashOperator(
        task_id='cleanup_test_dir',
        bash_command=f'rm -rf {LOCAL_RAW_DIR}/test'
    )
    
    setup >> download >> cleanup