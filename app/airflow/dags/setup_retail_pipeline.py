from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default 설정
default_args = {
    'owner': 'sorae',
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2025, 12, 1),
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

    reset_tables = BashOperator(
        task_id="reset_tables",
        bash_command="""
        docker exec postgres psql -U postgres -d retail_pipeline <<'SQL'

        -- 1. target interval 주문번호 기준 detail 삭제
        DELETE FROM order_detail
        WHERE invoice_no IN (
            SELECT invoice_no
            FROM order_info
            WHERE invoice_timestamp >= '{{dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}'
            AND invoice_timestamp <  '{{dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}'
        );

        -- 2. order_info 삭제
        DELETE FROM order_info
        WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}'
        AND invoice_timestamp <  '{{ dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}';

        -- 3. raw 삭제
        DELETE FROM raw_retail_events
        WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}'
        AND invoice_timestamp <  '{{ dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}';

        SQL

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
#        bash_command="docker start -a collector"
        bash_command="""
            docker compose run --rm \
            -e TARGET_START='{{ data_interval_start.strftime("%Y-%m-%d %H:%M:%S") }}' \
            -e TARGET_END='{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}' \
            collector
        """
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command="""
        docker exec postgres psql -U postgres -d retail_pipeline -c "
        SELECT COUNT(*) AS interval_count
        FROM raw_retail_events
        WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start", data_interval_start.strftime("%Y-%m-%d %H:%M:%S")) }}'
          AND invoice_timestamp <  '{{ dag_run.conf.get("target_end", data_interval_end.strftime("%Y-%m-%d %H:%M:%S")) }}';
        "
        """
    )

    reset_tables >> create_kafka_topic >> start_stream_raw_events >> check_stream_alive >> run_collector >> check_raw_count