import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def test_gcs_connection(**context):
    from google.cloud import storage
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/google_credentials.json'
    
    try:
        client = storage.Client()
        buckets = list(client.list_buckets())
        
        print(f"Successfully connected to GCS!")
        print(f"Found {len(buckets)} buckets:")
        for bucket in buckets:
            print(f"  - {bucket.name}")
        
        return True
        
    except Exception as e:
        print(f"Failed to connect to GCS: {str(e)}")
        raise


def test_bigquery_connection(**context):
    from google.cloud import bigquery
    
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/google_credentials.json'
    
    try:
        client = bigquery.Client()
        datasets = list(client.list_datasets())
        
        print(f"Successfully connected to BigQuery!")
        print(f"Found {len(datasets)} datasets:")
        for dataset in datasets:
            print(f"  - {dataset.dataset_id}")
        
        return True
        
    except Exception as e:
        print(f"Failed to connect to BigQuery: {str(e)}")
        raise


def test_binance_api(**context):
    import requests
    
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {
        "symbol": "BTCUSDT",
        "interval": "1d",
        "limit": 5
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"Successfully connected to Binance API!")
        print(f"Fetched {len(data)} klines for BTCUSDT")
        print(f"Latest close price: {data[-1][4]}")
        
        return True
        
    except Exception as e:
        print(f"Failed to connect to Binance API: {str(e)}")
        raise


with DAG(
    dag_id='test_connections',
    default_args={
        'owner': 'cryptolake',
        'depends_on_past': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
    },
    description='Test all external connections',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'validation'],
) as dag:
    
    check_env = BashOperator(
        task_id='check_environment',
        bash_command='''
        echo "Python version:"
        python --version
        echo ""
        echo "Checking for google_credentials.json:"
        ls -lh /opt/airflow/google_credentials.json
        echo ""
        echo "echo GOOGLE_APPLICATION_CREDENTIALS:"
        echo $GOOGLE_APPLICATION_CREDENTIALS
        echo ""
        echo "GCP Project ID:"
        echo $GCP_PROJECT_ID
        '''
    )
    
    test_gcs = PythonOperator(
        task_id='test_gcs_connection',
        python_callable=test_gcs_connection,
        provide_context=True
    )
    
    test_bq = PythonOperator(
        task_id='test_bigquery_connection',
        python_callable=test_bigquery_connection,
        provide_context=True
    )
    
    test_binance = PythonOperator(
        task_id='test_binance_api',
        python_callable=test_binance_api,
        provide_context=True
    )
    
    check_env >> [test_gcs, test_bq, test_binance]