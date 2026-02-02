import os
from datetime import datetime, timedelta

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "crypto-lake-486211")
GCP_REGION = os.getenv("GCP_REGION", "us-central1")

RAW_BUCKET = f"{GCP_PROJECT_ID}-crypto-raw"
PROCESSED_BUCKET = f"{GCP_PROJECT_ID}-crypto-processed"

BIGQUERY_DATASET = "crypto_market"
BIGQUERY_TABLE = "klines_daily"

TRADING_PAIRS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "DOTUSDT",
    "POLUSDT",  
    "LTCUSDT"
]

BINANCE_API_BASE = "https://api.binance.com"
BINANCE_BULK_BASE = "https://data.binance.vision"

KLINE_INTERVAL = "1d"

HISTORICAL_START_DATE = datetime(2020, 1, 1)
LOOKBACK_DAYS = 365

LOCAL_DATA_DIR = "/tmp/cryptolake"
LOCAL_RAW_DIR = f"{LOCAL_DATA_DIR}/raw"
LOCAL_PROCESSED_DIR = f"{LOCAL_DATA_DIR}/processed"

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")
GOOGLE_CREDENTIALS_PATH = f"{AIRFLOW_HOME}/google_credentials.json"

SPARK_APP_NAME = "CryptoLake-Processing"
SPARK_MASTER = "local[*]"

AIRFLOW_DEFAULT_ARGS = {
    "owner": "cryptolake",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

BIGQUERY_WRITE_DISPOSITION = "WRITE_APPEND"
BIGQUERY_CREATE_DISPOSITION = "CREATE_IF_NEEDED"