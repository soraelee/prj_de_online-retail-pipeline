from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# Default 설정
default_args = {
    'owner': 'sorae',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2025, 12, 1, tzinfo=KST),
}

with DAG(
    dag_id="setup_retail_pipeline",
    default_args=default_args,
    # schedule='@hourly',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:


    create_kafka_topic = BashOperator(
    task_id="create_kafka_topic",
    bash_command="""
        docker exec kafka kafka-topics \
        --bootstrap-server localhost:29092 \
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
        if ps -ef | grep stream_raw_events.py | grep -v grep; then
            echo "[STREAM] already running"
            exit 0
        fi

        echo "[STREAM] starting stream_raw_events.py"

        nohup /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 1 \
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


    create_kafka_topic >> start_stream_raw_events >> check_stream_alive