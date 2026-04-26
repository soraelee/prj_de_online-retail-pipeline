from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="setup_retail_pipeline",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
) as dag:

    reset_tables = BashOperator(
        task_id="reset_tables",
        bash_command="""
        docker exec postgres psql -U postgres -d retail_pipeline -c "
        TRUNCATE TABLE raw_retail_events RESTART IDENTITY CASCADE;
        TRUNCATE TABLE order_info RESTART IDENTITY CASCADE;
        TRUNCATE TABLE order_detail RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_customer RESTART IDENTITY CASCADE;
        TRUNCATE TABLE dim_product RESTART IDENTITY CASCADE;
        TRUNCATE TABLE mart_daily_orders RESTART IDENTITY CASCADE;
         "
        docker exec spark-master rm -rf /tmp/checkpoints/retail_events_raw
        """
    )

    create_kafka_topic = BashOperator(
    task_id="create_kafka_topic",
    bash_command="""
        docker exec kafka kafka-topics \
        --bootstrap-server kafka:29092 \
        --create \
        --if-not-exists \
        --topic retail-events \
        --partitions 1 \
        --replication-factor 1
        """
    )

    start_stream_raw_events = BashOperator(
        task_id="start_stream_raw_events",
        bash_command="""
        docker exec spark-master bash -lc '
        rm -f /tmp/stream_raw_events.log

        nohup /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 1g \
        --executor-cores 1 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --jars /opt/spark-jars/postgresql-42.7.3.jar \
        /opt/spark-apps/stream_raw_events.py \
        > /tmp/stream_raw_events.log 2>&1 &
        '
        """
    )

    check_stream_alive = BashOperator(
        task_id="check_stream_alive",
        bash_command="""
        sleep 10
        docker exec spark-master ps -ef | grep stream_raw_events | grep -v grep
        """
    )

    run_collector = BashOperator(
        task_id="run_collector",
        bash_command="docker start -a collector"
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command="""
        docker exec postgres psql -U postgres -d retail_pipeline -c "
        SELECT COUNT(*) FROM raw_retail_events;
        "
        """
    )

    reset_tables >> create_kafka_topic >> start_stream_raw_events >> check_stream_alive >> run_collector >> check_raw_count