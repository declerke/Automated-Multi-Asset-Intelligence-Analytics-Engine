.PHONY: help setup init up down restart logs clean terraform-init terraform-plan terraform-apply terraform-destroy test

setup:
	./setup.sh

init: setup
	cd terraform && terraform init

up:
	docker compose up -d
	sleep 10

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

terraform-init:
	cd terraform && terraform init

terraform-plan:
	cd terraform && terraform plan

terraform-apply:
	cd terraform && terraform apply

terraform-destroy:
	cd terraform && terraform destroy

test:
	docker exec -it $$(docker ps -qf "name=airflow-scheduler") airflow dags trigger test_connections

clean:
	rm -rf /tmp/cryptolake
	rm -rf airflow/logs/*

trigger-etl:
	docker exec -it $$(docker ps -qf "name=airflow-scheduler") airflow dags trigger cryptolake_etl_pipeline

list-dags:
	docker exec -it $$(docker ps -qf "name=airflow-scheduler") airflow dags list

airflow-shell:
	docker exec -it $$(docker ps -qf "name=airflow-scheduler") bash