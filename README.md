# 🪙 CryptoLake: Automated Multi-Asset Intelligence & Analytics Engine

**CryptoLake** is an institutional-grade data engineering solution designed to transform volatile, high-frequency cryptocurrency market data into professional business intelligence. It automates the ingestion of Binance market data, processes it via a containerized PySpark engine, and delivers live insights through a Google BigQuery-backed Looker Studio dashboard.

---

## 🎯 Project Goal
To build a scalable, cloud-native ETL pipeline that handles the "Volume, Velocity, and Variety" of crypto markets. The project demonstrates automated daily orchestration, rigorous time-series feature engineering (moving averages, volatility, correlations), and professional BI delivery tailored for advanced financial analytics.

---

## 🧬 System Architecture
The pipeline follows a robust **Bronze-Silver-Gold** medallion architecture built on an "Orchestrate-Transform-Visualize" logic:

1.  [cite_start]**Orchestration (Airflow):** A Dockerized Airflow 2.10.x instance manages the `cryptolake_etl_pipeline` DAG, ensuring tasks like ingestion, transformation, and cloud loading trigger sequentially with built-in retry logic[cite: 1, 2].
2.  **Processing (PySpark):** Extracts OHLCV (Open, High, Low, Close, Volume) data from Binance endpoints. [cite_start]Spark handles schema enforcement and calculates complex financial features like 30-day rolling windows[cite: 1, 2].
3.  [cite_start]**Data Lake (GCS):** Raw and processed Parquet files are stored in Google Cloud Storage, organized by `date/symbol` for optimal retrieval[cite: 1].
4.  [cite_start]**Warehouse (BigQuery):** Data is materialized into partitioned tables, allowing for high-performance SQL analysis and cost-efficient BI consumption[cite: 1].
5.  **BI Layer:** Interactive Looker Studio dashboards connected directly to BigQuery for real-time reporting.

---

## 📊 Dashboard Preview
![Correlation Heatmap](docs/correlation_heatmap.jpg)
> *Correlation Heatmap identifying price movement relationships between BTC and Altcoins.*

![Price Trend Analysis](docs/price_trends.jpg)
> *Interactive trend analysis showing 'Close Over Time' with a 30-day moving average overlay.*

---

## 🛠️ Technical Stack
| Layer | Tool / Library | Purpose / Role |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 2.10.x | [cite_start]DAG scheduling, task dependencies, and retry logic[cite: 1]. |
| **Data Processing** | Spark (PySpark) | [cite_start]Scalable DataFrame transformations and financial window functions[cite: 1]. |
| **Cloud Platform** | Google Cloud Platform | [cite_start]Hosts the GCS data lake and BigQuery warehouse[cite: 1]. |
| **Infrastructure** | Docker & Compose | [cite_start]Containerized Airflow environment for 100% reproducibility[cite: 1]. |
| **IaC** | Terraform ~1.14.x | [cite_start]Declarative management of GCS buckets and BigQuery datasets[cite: 1]. |
| **Language** | Python 3.10 | [cite_start]Core logic for DAGs, API calls, and custom ETL functions[cite: 1]. |
| **Analytics** | Looker Studio | Executive-grade visualization of market KPIs. |

---

## 📂 Project Structure
```text
cryptolake-gcp/
├── airflow/
│   ├── dags/                   # ETL Pipeline (cryptolake_etl_dag.py)
[cite_start]│   └── logs/                   # Task execution & attempt history [cite: 1, 2]
[cite_start]├── spark/                      # Local PySpark transformation logic [cite: 1]
[cite_start]├── terraform/                  # IaC (main.tf, variables.tf, providers) [cite: 1]
[cite_start]├── docker-compose.yml          # Container orchestration for Airflow stack [cite: 1]
[cite_start]├── google_credentials.json     # Secure GCP Service Account access [cite: 1]
[cite_start]├── requirements.txt            # Python dependency manifest [cite: 1]
└── README.md
```

---

## ⚙️ Installation & Setup

### 1. Launch the Stack
Initialize the containerized environment to start the Airflow scheduler and webserver:
```bash
docker-compose up -d
```

### 2. Infrastructure Deployment
Use Terraform to provision the necessary Google Cloud Storage buckets and BigQuery datasets:
```bash
cd terraform
terraform init
terraform apply
```

---

## 🎓 Skills Demonstrated
* **Modern Data Stack:** Practical experience with Airflow, Spark, Docker, and Terraform.
* **Cloud Architecture:** Designing partitioned storage systems in GCP to optimize performance and cost.
* **Financial Engineering:** Implementing statistical measures (CORR, STDDEV) to extract signal from market noise.
* **Data Reliability:** Handling high-frequency data challenges, including timestamp precision and API rate-limiting.
