"""
Retail Pipeline DAG
- raw_retail_events는 Spark Streaming으로 계속 적재
- batch_dim: dim_customer, dim_product 매일 갱신
- batch_mart: mart_daily_orders, mart_product_sales 매일 갱신
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Default 설정
default_args = {
    'owner': 'sorae',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 12, 1),
}

dag = DAG(
    'retail_pipeline',
    default_args=default_args,
    description='Retail Data Pipeline: Dimension & Mart Build',
    schedule="30 0 * * *",
    catchup=False,
)

# Spark 마스터 URI
SPARK_MASTER = "spark://spark-master:7077"

reset_tables = BashOperator(
    task_id="reset_tables",
    bash_command=f"""
        docker exec postgres psql -U postgres -d retail_pipeline <<'SQL'

        -- 1. dim_customer 삭제
        TRUNCATE TABLE dim_customer;

        -- 2. dim_product 삭제
        TRUNCATE TABLE dim_product;

        -- 3. dim_customer rebuild
        INSERT INTO dim_customer (
            customer_id,
            first_purchase_at,
            last_purchase_at,
            total_order_count,
            country
        )
        SELECT
            customer_id,
            MIN(invoice_timestamp) AS first_purchase_at,
            MAX(invoice_timestamp) AS last_purchase_at,
            COUNT(DISTINCT invoice_no) AS total_order_count,
            MAX(country) AS country
        FROM raw_retail_events
        WHERE customer_id IS NOT NULL
        AND event_type = 'order'
        GROUP BY customer_id;

        -- 4. dim_product rebuild
        INSERT INTO dim_product (
            stock_code,
            category,
            description,
            latest_unit_price
        )
        SELECT
            stock_code,
            MAX(category) AS category,
            MAX(description) AS description,
            MAX(unit_price) AS latest_unit_price
        FROM raw_retail_events
        WHERE stock_code IS NOT NULL
        GROUP BY stock_code;

        -- 5. 해당 날짜 mart 삭제
        DELETE FROM mart_daily_orders
        WHERE order_date = '{{ data_interval_start.strftime("%Y-%m-%d") }}';

        -- 6. 해당 날짜 mart product 삭제
        DELETE FROM mart_product_sales
        WHERE order_date = '{{ data_interval_start.strftime("%Y-%m-%d") }}';

        -- 7. 해당 날짜 mart customer repeat 삭제
        DELETE FROM mart_customer_repeats
        WHERE order_date = '{{ data_interval_start.strftime("%Y-%m-%d") }}';

        SQL

        docker exec spark-master rm -rf /tmp/checkpoints/retail_events_raw
    """,
    dag=dag,
)

# Task: dim 생성 (dim_customer, dim_product)
build_dim = BashOperator(
    task_id='build_dim',
    bash_command=f"""
        spark-submit \
            --master {SPARK_MASTER} \
            --deploy-mode client \
            /opt/spark-apps/build_dim.py \
            --start '{{{{ data_interval_start.strftime("%Y-%m-%d") }}}}' \
            --end '{{{{ data_interval_end.strftime("%Y-%m-%d") }}}}'
    """,
    dag=dag,
)

# Task: mart 생성 (mart_daily_orders, mart_product_sales)
build_mart = BashOperator(
    task_id='build_mart',
    bash_command=f"""
        spark-submit \
            --master {SPARK_MASTER} \
            --deploy-mode client \
            /opt/spark-apps/build_mart.py \
            --start '{{{{ data_interval_start.strftime("%Y-%m-%d") }}}}' \
            --end '{{{{ data_interval_end.strftime("%Y-%m-%d") }}}}'
    """,
    dag=dag,
    depends_on_past=False,
)

# Task 순서: reset_tables -> build_dim -> build_mart
reset_tables >> build_dim >> build_mart
