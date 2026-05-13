"""
일별 Retail 요약 Slack 알림 DAG

- 매일 오전 8시 30분에 실행
- 전일 주문/취소/고객/매출 요약을 Slack으로 전송
"""

import sys
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/retail-pipeline")

from slack_notifier import notify_dag_failure, send_daily_summary_alert


KST = pendulum.timezone("Asia/Seoul")


default_args = {
    "owner": "sorae",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_dag_failure,
}


def run_daily_summary_alert(**context):
    target_date = (
        context["data_interval_end"]
        .in_timezone("Asia/Seoul")
        .subtract(days=1)
        .strftime("%Y-%m-%d")
    )

    send_daily_summary_alert(target_date)


with DAG(
    dag_id="daily_retail_summary_alert",
    default_args=default_args,
    start_date=datetime(2025, 12, 1, tzinfo=KST),
    schedule="30 8 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    send_daily_summary = PythonOperator(
        task_id="send_daily_summary",
        python_callable=run_daily_summary_alert,
    )
