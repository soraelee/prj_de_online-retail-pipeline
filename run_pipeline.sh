#!/bin/bash
set -e

echo "1. Start containers"
docker compose up -d zookeeper kafka postgres spark-master spark-worker airflow airflow-scheduler

echo "2. Create collector container without starting"
docker compose up --no-start collector

echo "3. Wait for Airflow to load DAGs"
sleep 10

echo "4. Show DAG list"
docker compose exec airflow airflow dags list

echo "5. Unpause setup DAG"
docker compose exec airflow airflow dags unpause setup_retail_pipeline

echo "6. Trigger setup DAG"
docker compose exec airflow airflow dags trigger setup_retail_pipeline

echo "7. Unpause build DAG"
docker compose exec airflow airflow dags unpause retail_pipeline

echo "8. Trigger build DAG"
docker compose exec airflow airflow dags trigger retail_pipeline

echo "9. Airflow UI: http://localhost:8081"