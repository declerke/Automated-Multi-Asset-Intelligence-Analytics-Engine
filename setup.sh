#!/bin/bash

set -e

if [ ! -f .env ]; then
    cp .env.example .env
fi

if [ ! -f google_credentials.json ]; then
    exit 1
fi

if [ ! -f terraform/terraform.tfvars ]; then
    cp terraform/terraform.tfvars.example terraform/terraform.tfvars
fi

mkdir -p airflow/logs airflow/plugins /tmp/cryptolake

echo "AIRFLOW_UID=$(id -u)" >> .env