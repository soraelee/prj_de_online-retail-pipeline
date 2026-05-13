#!/bin/bash
set -e

echo "1. Start containers"
docker compose up -d zookeeper kafka postgres spark-master spark-worker airflow airflow-scheduler collector

# echo "2. Create collector container without starting"
# docker compose up --no-start collector

echo "2. Wait for Airflow to load DAGs"
sleep 10

echo "3. Show DAG list"
docker compose exec airflow airflow dags list

echo "4. Unpause setup DAG"
docker compose exec airflow airflow dags unpause setup_retail_pipeline

echo "5. Trigger setup DAG"
docker compose exec airflow airflow dags trigger setup_retail_pipeline

echo "6. Unpause hourly DAG"
docker compose exec airflow airflow dags unpause hourly_retail_ingestion

# echo "7. Trigger hourly DAG"
# docker compose exec airflow airflow dags trigger hourly_retail_ingestion

echo "7. Unpause build DAG"
docker compose exec airflow airflow dags unpause retail_pipeline

# echo "9. Trigger build DAG"
# docker compose exec airflow airflow dags trigger retail_pipeline

echo "8. Unpause backfill DAG"
docker compose exec airflow airflow dags unpause backfill_retail_jsonl

# echo "11. Trigger backfill DAG"
# docker compose exec airflow airflow dags trigger backfill_retail_jsonl

echo "9. Airflow UI: http://localhost:8081"

echo "10. Set API environment variables"
docker compose up --build -d api

echo "11. Set Dashboard environment variables"
docker compose up --build -d dashboard