"""
Backfill Retail JSONL DAG
- 목적: 과거 데이터 범위에 대한 JSONL 파일을 읽어와서 Kafka로 전송하여 Spark Streaming이 처리할 수 있도록 함
- JSONL 파일은 invoice_timestamp 기준으로 분할되어 저장되어 있다고 가정
- DAG 실행 시, 대상 기간을 지정하여 해당 기간에 해당하는 JSONL 파일들을 읽어서 Kafka로 전송
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from textwrap import dedent
import pendulum
import sys

sys.path.insert(0, "/opt/retail-pipeline")

from slack_notifier import notify_dag_failure

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sorae",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": notify_dag_failure,
}

with DAG(
    dag_id="backfill_retail_jsonl",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="30 0 * * *",  # 매일 00:30에 실행
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    
    # Spark 마스터 URI
    SPARK_MASTER = "spark://spark-master:7077"

    create_kafka_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=dedent("""
            docker exec kafka kafka-topics \
                --bootstrap-server kafka:29092 \
                --create \
                --if-not-exists \
                --topic retail-events \
                --partitions 1 \
                --replication-factor 1
        """)
    )

    check_spark_streaming = BashOperator(
        task_id="check_spark_streaming",
        bash_command=dedent("""
            sleep 10
            docker exec spark-master ps -ef | grep stream_raw_events | grep -v grep
        """)
    )

    prepare_jsonl_file = BashOperator(
        task_id="prepare_jsonl_file",
        bash_command=dedent("""
            docker exec collector bash -lc '
                mkdir -p /app/fallback/pending /app/fallback/processing /app/fallback/processed /app/fallback/error

                if [ ! -s /app/fallback/pending/failed_messages.jsonl ]; then
                    echo "[SKIP] no fallback jsonl file"
                    exit 99
                fi

                TS=$(date +%Y%m%d_%H%M%S)
                mv /app/fallback/pending/failed_messages.jsonl /app/fallback/processing/failed_messages_${TS}.jsonl

                echo "/app/fallback/processing/failed_messages_${TS}.jsonl" > /app/fallback/processing/latest_processing_file.txt
                cat /app/fallback/processing/latest_processing_file.txt
            '
        """),
        skip_on_exit_code=99,
    )

    replay_jsonl_to_kafka = BashOperator(
        task_id="replay_jsonl_to_kafka",
        bash_command=dedent("""
            docker exec collector bash -lc '
            FALLBACK_PATH=$(cat /app/fallback/processing/latest_processing_file.txt)

            echo "[REPLAY FILE] $FALLBACK_PATH"

            FALLBACK_PATH=$FALLBACK_PATH \
            KAFKA_TOPIC=retail-events \
            KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
            python /app/replay_fallback_jsonl.py
            '
        """)
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command=dedent("""
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

            echo "[check_raw_count] raw count is still 0"
            exit 1
        """)
    )

    # Task: dim 생성 (dim_customer, dim_product)
    run_batch_build_dim = BashOperator(
        task_id="build_dim",
        bash_command=f"""
            docker exec spark-master bash -lc '
            /opt/bitnami/spark/bin/spark-submit \
                --master {SPARK_MASTER} \
                --deploy-mode client \
                --executor-cores 1 \
                --executor-memory 1g \
                --total-executor-cores 1 \
                --jars /opt/spark-jars/postgresql-42.7.3.jar \
                --driver-class-path /opt/spark-jars/postgresql-42.7.3.jar \
                /opt/spark-apps/build_dim.py \
                --start "{{{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S')) }}}}" \
                --end "{{{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S')) }}}}"
            '
        """,
        dag=dag,
    )

    # Task: mart 생성 (mart_daily_orders, mart_product_sales)
    run_batch_build_mart = BashOperator(
        task_id="build_mart",
        bash_command=f"""
            docker exec spark-master bash -lc '
            /opt/bitnami/spark/bin/spark-submit \
                --master {SPARK_MASTER} \
                --deploy-mode client \
                --executor-cores 1 \
                --executor-memory 1g \
                --total-executor-cores 1 \
                --jars /opt/spark-jars/postgresql-42.7.3.jar \
                --driver-class-path /opt/spark-jars/postgresql-42.7.3.jar \
                /opt/spark-apps/build_mart.py \
                --start "{{{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S')) }}}}" \
                --end "{{{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S')) }}}}"
            '
        """,
        dag=dag,
        depends_on_past=False,
    )

    # Task: 집계 결과 검증 (dim, mart)
    check_agg_count = BashOperator(
        task_id="check_agg_count",
        bash_command=dedent("""
            docker exec postgres psql -U postgres -d retail_pipeline -c "
            SELECT '{{ dag_run.conf.get("target_start") }}' AS order_date, table_name, row_count
            FROM (               
                SELECT 'dim_customer' AS table_name, COUNT(*) AS row_count
                FROM dim_customer

                UNION ALL

                SELECT 'dim_product' AS table_name, COUNT(*) AS row_count
                FROM dim_product

                UNION ALL

                SELECT 'mart_daily_orders' AS table_name, COUNT(*) AS row_count
                FROM mart_daily_orders
                WHERE order_date >= '{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'

                UNION ALL

                SELECT 'mart_product_sales' AS table_name, COUNT(*) AS row_count
                FROM mart_product_sales
                WHERE order_date >= '{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'
                                    
                UNION ALL

                SELECT 'mart_customer_repeat' AS table_name, COUNT(*) AS row_count
                FROM mart_customer_repeat
                WHERE order_date >= '{{ dag_run.conf.get("target_start", data_interval_start.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end", data_interval_end.in_timezone("Asia/Seoul").strftime('%Y-%m-%d %H:%M:%S'))[:10] }}'
            ) AS counts;
            "
        """)
    )

    # 성공 시 처리된 JSONL 파일을 /processed로 이동
    archive_processed_jsonl = BashOperator(
        task_id="archive_processed_jsonl",
        bash_command=dedent("""
            docker exec collector bash -lc '
            FALLBACK_PATH=$(cat /app/fallback/processing/latest_processing_file.txt)
            FILE_NAME=$(basename "$FALLBACK_PATH")

            mv "$FALLBACK_PATH" "/app/fallback/processed/$FILE_NAME"
            rm -f /app/fallback/processing/latest_processing_file.txt

            echo "[ARCHIVED] /app/fallback/processed/$FILE_NAME"
            '
        """)
    )

    # 실패 시 처리된 JSONL 파일을 /error로 이동
    archive_error_jsonl = BashOperator(
        task_id="archive_error_jsonl",
        bash_command=dedent("""
            docker exec collector bash -lc '
            FALLBACK_PATH=$(cat /app/fallback/processing/latest_processing_file.txt)
            FILE_NAME=$(basename "$FALLBACK_PATH")

            mv "$FALLBACK_PATH" "/app/fallback/error/$FILE_NAME"
            rm -f /app/fallback/processing/latest_processing_file.txt

            echo "[ARCHIVED] /app/fallback/error/$FILE_NAME"
            '
        """),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    create_kafka_topic >> check_spark_streaming >> prepare_jsonl_file >> replay_jsonl_to_kafka \
    >> check_raw_count >> run_batch_build_dim >> run_batch_build_mart >> check_agg_count

    check_agg_count >> archive_processed_jsonl
    check_agg_count >> archive_error_jsonl
