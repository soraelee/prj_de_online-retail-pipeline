"""
hourly_retail_ingestion DAG
- 목적: 매 시간마다 target interval 데이터를 Collector를 통해 Kafka로 발행하고,
        Spark Streaming이 raw_retail_events에 적재하는지 확인한다.
- 예시: 01:00에 실행되는 DAG는 00:00부터 01:00까지의 데이터를 처리 대상으로 한다.
- order_info/order_detail, dim, mart 생성은 별도 batch DAG 또는 backfill DAG에서 수행한다.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
import sys

sys.path.insert(0, "/opt/retail-pipeline")

from slack_notifier import notify_dag_failure

KST = pendulum.timezone("Asia/Seoul")

# Default 설정
default_args = {
    'owner': 'sorae',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
    'start_date': datetime(2025, 12, 1, tzinfo=KST),
    'on_failure_callback': notify_dag_failure,
}

with DAG(
    dag_id="hourly_retail_ingestion",
    default_args=default_args,
    schedule='@hourly',
    # schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:

    check_stream_alive = BashOperator(
        task_id="check_stream_alive",
        bash_command="""
        docker exec spark-master bash -lc '
        if ps -ef | grep stream_raw_events.py | grep -v grep; then
            echo "[STREAM] already running"
            exit 0
        fi

        echo "[STREAM] not running. starting stream_raw_events.py"

        nohup /opt/bitnami/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --executor-memory 1g \
        --executor-cores 1 \
        --total-executor-cores 1 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
        --jars /opt/spark-jars/postgresql-42.7.3.jar \
        /opt/spark-apps/stream_raw_events.py \
        > /tmp/stream_raw_events.log 2>&1 &

        sleep 10

        ps -ef | grep stream_raw_events.py | grep -v grep
        '
        """
    )

    run_collector = BashOperator(
    task_id="run_collector",
    bash_command="""
    docker exec \
        -e TARGET_START='{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")) }}' \
        -e TARGET_END='{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")) }}' \
        collector \
        python /app/producer.py
        """
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command="""
            for i in $(seq 1 30)
            do
                COUNT=$(docker exec postgres psql -U postgres -d retail_pipeline -t -A -c "
                SELECT COUNT(*)
                FROM raw_retail_events
                WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")) }}'
                    AND invoice_timestamp <  '{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")) }}';
                ")

                echo "[check_raw_count] attempt=$i count=$COUNT"

                if [ "$COUNT" -gt 0 ]; then
                    exit 0
                fi

                sleep 10
            done

            echo "[check_raw_count] raw count is 0. No data for this interval, treated as success."
            exit 0
        """
    )

    check_stream_alive >> run_collector >> check_raw_count
