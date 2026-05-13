"""
Slack 알림 공통 모듈

1. 일별 데이터 요약 알림
- 전일 전체 주문건수/취소건수
- 전일 신규 고객 수
- 전일 순 매출
- 전일 고객 재구매율 및 리턴 고객 수

2. Airflow DAG 운영 중 오류 발생 시 알림
"""

import os
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras
import requests


def get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return value


class SlackNotifier:
    def __init__(self, webhook_url: Optional[str] = None, channel: Optional[str] = None):
        self.webhook_url = webhook_url or get_env("SLACK_WEBHOOK_URL")
        self.channel = channel or get_env("SLACK_CHANNEL")

    def send_message(self, message: str) -> None:
        if not self.webhook_url:
            raise RuntimeError("SLACK_WEBHOOK_URL이 설정되어 있지 않습니다.")

        payload = {"text": message}
        if self.channel:
            payload["channel"] = self.channel

        response = requests.post(self.webhook_url, json=payload, timeout=10)
        response.raise_for_status()

    def build_daily_summary_message(self, summary: Dict[str, Any]) -> str:
        target_date = summary.get("order_date") or "-"
        order_cnt = int(summary.get("order_cnt") or 0)
        cancel_cnt = int(summary.get("cancel_cnt") or 0)
        new_customer_cnt = int(summary.get("new_customer_cnt") or 0)
        repeat_customer_cnt = int(summary.get("repeat_customer_cnt") or 0)
        repeat_customer_rate = float(summary.get("repeat_customer_rate") or 0)
        total_sales_amount = float(summary.get("total_sales_amount") or 0)

        return (
            f"*일별 데이터 요약 ({target_date})*\n"
            f"- 전체 주문건수: {order_cnt:,}건\n"
            f"- 전체 취소건수: {cancel_cnt:,}건\n"
            f"- 신규 고객 수: {new_customer_cnt:,}명\n"
            f"- 순 매출: {total_sales_amount:,.2f}\n"
            f"- 고객 재구매율: {repeat_customer_rate:.2f}% "
            f"(리턴 고객 {repeat_customer_cnt:,}명)"
        )

    def build_failure_message(self, context: Dict[str, Any]) -> str:
        task_instance = context.get("task_instance")
        dag = context.get("dag")
        exception = context.get("exception")

        dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "-"
        task_id = getattr(task_instance, "task_id", "-")
        run_id = context.get("run_id") or getattr(task_instance, "run_id", "-")
        log_url = getattr(task_instance, "log_url", "-")

        return (
            "*Airflow DAG 오류 발생*\n"
            f"- DAG: {dag_id}\n"
            f"- Task: {task_id}\n"
            f"- Run ID: {run_id}\n"
            f"- 발생 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"- 오류 내용: {exception}\n"
            f"- 로그: {log_url}"
        )


def get_postgres_connection():
    return psycopg2.connect(
        host=get_env("DB_HOST", "postgres"),
        port=get_env("DB_PORT", "5432"),
        dbname=get_env("DB_NAME", "retail_pipeline"),
        user=get_env("DB_USER", "postgres"),
        password=get_env("DB_PASSWORD", "postgres"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def fetch_daily_summary(target_date: str) -> Dict[str, Any]:
    sql = """
        WITH daily AS (
            SELECT
                order_date,
                order_cnt,
                cancel_cnt,
                total_sales_amount
            FROM mart_daily_orders
            WHERE order_date = %(target_date)s::date
        ),
        new_customers AS (
            SELECT
                first_purchase_at::date AS order_date,
                COUNT(DISTINCT customer_id) AS new_customer_cnt
            FROM dim_customer
            WHERE first_purchase_at::date = %(target_date)s::date
            GROUP BY first_purchase_at::date
        )
        SELECT
            COALESCE(d.order_date, n.order_date, r.order_date, %(target_date)s::date) AS order_date,
            COALESCE(d.order_cnt, 0) AS order_cnt,
            COALESCE(d.cancel_cnt, 0) AS cancel_cnt,
            COALESCE(d.total_sales_amount, 0) AS total_sales_amount,
            COALESCE(n.new_customer_cnt, 0) AS new_customer_cnt,
            COALESCE(r.repeat_customer_cnt, 0) AS repeat_customer_cnt,
            COALESCE(r.repeat_customer_rate, 0) AS repeat_customer_rate
        FROM daily d
        FULL OUTER JOIN new_customers n ON d.order_date = n.order_date
        FULL OUTER JOIN mart_customer_repeat r
            ON COALESCE(d.order_date, n.order_date) = r.order_date
        WHERE COALESCE(d.order_date, n.order_date, r.order_date, %(target_date)s::date)
            = %(target_date)s::date
    """

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, {"target_date": target_date})
            row = cur.fetchone()

    if row:
        return dict(row)

    return {
        "order_date": target_date,
        "order_cnt": 0,
        "cancel_cnt": 0,
        "total_sales_amount": 0,
        "new_customer_cnt": 0,
        "repeat_customer_cnt": 0,
        "repeat_customer_rate": 0,
    }


def send_daily_summary_alert(target_date: Optional[str] = None) -> None:
    if target_date is None:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    notifier = SlackNotifier()
    summary = fetch_daily_summary(target_date)
    message = notifier.build_daily_summary_message(summary)
    notifier.send_message(message)


def notify_dag_failure(context: Dict[str, Any]) -> None:
    notifier = SlackNotifier()
    message = notifier.build_failure_message(context)
    notifier.send_message(message)
