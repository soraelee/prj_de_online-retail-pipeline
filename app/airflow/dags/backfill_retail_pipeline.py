'''
reset_target_range
↓
create_kafka_topic
↓
run_collector_for_range
↓
check_raw_count
↓
run_batch_build_dim
↓
run_batch_build_mart
↓
check_agg_count

'''
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    "owner": "sorae",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="backfill_retail_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
) as dag:
    
    # Spark 마스터 URI
    SPARK_MASTER = "spark://spark-master:7077"
    
    #stream이 잘 켜져있는 지 확인
    check_spark_streaming = BashOperator(
        task_id="check_spark_streaming",
        bash_command=dedent("""
            docker exec spark-master bash -lc "
            ps -ef | grep stream_raw_events | grep -v grep
            "
        """)
    )

    reset_target_range = BashOperator(
        task_id="reset_target_range",
        bash_command=dedent("""
            docker exec postgres psql -v ON_ERROR_STOP=1 -U postgres -d retail_pipeline <<'SQL'
            DELETE FROM order_detail
            WHERE invoice_no IN (
                SELECT invoice_no
                FROM order_info
                WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
                  AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}'
            );

            DELETE FROM order_info
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';

            DELETE FROM raw_retail_events
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';
            SQL
        """)
    )

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

    run_collector_for_range = BashOperator(
        task_id="run_collector_for_range",
        bash_command=dedent("""
            docker exec \
              -e TARGET_START='{{ dag_run.conf.get("target_start") }}' \
              -e TARGET_END='{{ dag_run.conf.get("target_end") }}' \
              collector \
            python /app/producer.py
        """)
    )

    check_raw_count = BashOperator(
        task_id="check_raw_count",
        bash_command=dedent("""
            docker exec postgres psql -U postgres -d retail_pipeline -c "
            SELECT COUNT(*) AS raw_count
            FROM raw_retail_events
            WHERE invoice_timestamp >= '{{ dag_run.conf.get("target_start") }}'
              AND invoice_timestamp <  '{{ dag_run.conf.get("target_end") }}';
            "
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
                --start "{{{{ dag_run.conf.get("target_start") }}}}" \
                --end "{{{{ dag_run.conf.get("target_end") }}}}"
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
                --start "{{{{ dag_run.conf.get("target_start") }}}}" \
                --end "{{{{ dag_run.conf.get("target_end") }}}}"
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
                WHERE order_date >= '{{ dag_run.conf.get("target_start")[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end")[:10] }}'

                UNION ALL

                SELECT 'mart_product_sales' AS table_name, COUNT(*) AS row_count
                FROM mart_product_sales
                WHERE order_date >= '{{ dag_run.conf.get("target_start")[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end")[:10] }}'
                                    
                UNION ALL

                SELECT 'mart_customer_repeat' AS table_name, COUNT(*) AS row_count
                FROM mart_customer_repeat
                WHERE order_date >= '{{ dag_run.conf.get("target_start")[:10] }}'
                AND order_date <  '{{ dag_run.conf.get("target_end")[:10] }}'
            ) AS counts;
            "
        """)
    )


    check_spark_streaming >>reset_target_range >> create_kafka_topic \
    >> run_collector_for_range >> check_raw_count >> run_batch_build_dim >> run_batch_build_mart >> check_agg_count   