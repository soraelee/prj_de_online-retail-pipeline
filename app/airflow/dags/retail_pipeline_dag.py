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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'retail_pipeline',
    default_args=default_args,
    description='Retail Data Pipeline: Dimension & Mart Build',
    schedule_interval=None,
    catchup=False,
)

# Spark 마스터 URI
SPARK_MASTER = "spark://spark-master:7077"

# Task: dim 생성 (dim_customer, dim_product)
build_dim = BashOperator(
    task_id='build_dim',
    bash_command=f"""
        spark-submit \
            --master {SPARK_MASTER} \
            --deploy-mode client \
            /opt/spark-apps/build_dim.py \
            --start '{{{{ dag_run.conf["start"] }}}}' \
            --end '{{{{ dag_run.conf["end"] }}}}'
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
            --start '{{{{ dag_run.conf["start"] }}}}' \
            --end '{{{{ dag_run.conf["end"] }}}}'
    """,
    dag=dag,
    depends_on_past=False,
)

# Task 순서: dim -> mart
build_dim >> build_mart
