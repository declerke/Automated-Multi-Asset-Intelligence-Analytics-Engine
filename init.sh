#!/bin/bash

set -e

BLUE='\033[0;34m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

check_command() {
    if command -v $1 &> /dev/null; then
        echo -e "${GREEN}✓${NC} $1 is installed"
        return 0
    else
        echo -e "${RED}✗${NC} $1 is not installed"
        return 1
    fi
}

echo -e "${BLUE}==> Checking prerequisites...${NC}"
echo ""

MISSING_DEPS=0

check_command "docker" || MISSING_DEPS=1
check_command "docker-compose" || check_command "docker" && docker compose version &> /dev/null || MISSING_DEPS=1
check_command "terraform" || MISSING_DEPS=1
check_command "python3" || MISSING_DEPS=1
check_command "gcloud" || echo -e "${YELLOW}⚠${NC} gcloud CLI not installed"

echo ""

if [ $MISSING_DEPS -eq 1 ]; then
    echo -e "${RED}Error: Missing required dependencies.${NC}"
    exit 1
fi

echo -e "${GREEN}All prerequisites met!${NC}"
echo ""

mkdir -p airflow/logs airflow/plugins /tmp/cryptolake/raw /tmp/cryptolake/processed

if [ ! -f .env ]; then
    cp .env.example .env
    AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
fi

if [ ! -f google_credentials.json ]; then
    echo -e "${RED}✗${NC} google_credentials.json not found!"
    exit 1
else
    chmod 600 google_credentials.json
fi

if [ ! -f terraform/terraform.tfvars ]; then
    cp terraform/terraform.tfvars.example terraform/terraform.tfvars
fi

if [ -f .env ]; then
    if grep -q "your-gcp-project-id" .env; then
        echo -e "${YELLOW}⚠${NC} .env needs configuration"
    fi
fi

if [ -f terraform/terraform.tfvars ]; then
    if grep -q "your-gcp-project-id" terraform/terraform.tfvars; then
        echo -e "${YELLOW}⚠${NC} terraform.tfvars needs configuration"
    fi
fi

echo ""
echo -e "${GREEN}Setup Complete! 🚀${NC}"