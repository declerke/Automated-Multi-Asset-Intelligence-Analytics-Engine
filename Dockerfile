FROM apache/airflow:2.10.4-python3.10

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN rm -rf ~/.ivy2/cache ~/.m2/repository

RUN pip install pyspark==3.5.3 requests==2.32.3 google-cloud-storage==2.18.2 google-cloud-bigquery==3.26.0 pandas==2.2.3 pyarrow==17.0.0