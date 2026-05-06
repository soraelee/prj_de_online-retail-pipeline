import os
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query, HTTPException


app = FastAPI(
    title="Retail Pipeline API",
    description="Kafka/Spark/Airflow로 적재된 Retail 데이터를 조회하는 API",
    version="1.0.0",
)


DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "retail_pipeline")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def normalize_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalize_rows(rows):
    return [
        {key: normalize_value(value) for key, value in row.items()}
        for row in rows
    ]


def fetch_all(sql: str, params: dict = None):
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or {})
                rows = cur.fetchall()
                return normalize_rows(rows)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def fetch_one(sql: str, params: dict = None):
    rows = fetch_all(sql, params)
    if not rows:
        return None
    return rows[0]


@app.get("/health")
def health_check():
    sql = "SELECT 1 AS ok"
    result = fetch_one(sql)

    return {
        "status": "ok",
        "db": result,
    }


@app.get("/api/v1/summary/counts")
def get_table_counts(
    start_date: Optional[str] = Query(None, example="2025-12-01"),
    end_date: Optional[str] = Query(None, example="2025-12-31")
):
    """
    현재 주요 테이블에 데이터가 얼마나 적재되어 있는지 확인하는 API
    """
    sql = """
        SELECT 'order_info' AS table_name, COUNT(*) AS row_count
        FROM raw_retail_events
        WHERE invoice_date >= %(start_date)s::timestamp
          AND invoice_date <  %(end_date)s::timestamp

        UNION ALL

        SELECT 'mart_daily_orders' AS table_name, COUNT(*) AS row_count
        FROM mart_daily_orders
        WHERE order_date >= %(start_date)s::date
          AND order_date <  %(end_date)s::date

        UNION ALL

        SELECT 'mart_product_sales' AS table_name, COUNT(*) AS row_count
        FROM mart_product_sales
        WHERE order_date >= %(start_date)s::date
          AND order_date <  %(end_date)s::date
    """

    rows = fetch_all(sql, {
        "start_date": start_date,
        "end_date": end_date,
    })

    return {
        "data": rows
    }

@app.get("/api/v1/daily-order-info")
def get_daily_order_info(
    start_date: Optional[str] = Query(None, example="2025-12-01"),
    end_date: Optional[str] = Query(None, example="2025-12-31"),
    event_type: Optional[str] = Query(None, example="order", regex="^(order|cancel)$"),
):
    """
    일별/시간대별 주문 정보 API
     - order_info 테이블에 적재된 일별/ 시간대별 주문 정보를 조회하는 API
    """
    sql = """
        SELECT
            invoice_date,
            EXTRACT(HOUR FROM invoice_time::time) AS invoice_hour,
            COUNT(a.invoice_no) AS order_count,
            SUM(b.quantity) AS total_quantity,
            SUM(b.quantity * b.unit_price) AS total_revenue        
        FROM order_info a
        LEFT JOIN order_detail b ON a.invoice_no = b.invoice_no 
        WHERE (%(start_date)s IS NULL OR invoice_date >= %(start_date)s::date)
          AND (%(end_date)s IS NULL OR invoice_date <= %(end_date)s::date)
          AND (%(event_type)s IS NULL OR a.event_type = %(event_type)s)
          GROUP BY invoice_date, invoice_hour
        ORDER BY invoice_date ASC, invoice_hour ASC
    """

    rows = fetch_all(sql, {
        "start_date": start_date,
        "end_date": end_date,
        "event_type": event_type,
    })

    return {
        "data": rows
    }

@app.get("/api/v1/summary/daily-orders")
def get_order_info_summary(
    start_date: Optional[str] = Query(None, example="2025-12-01"),
    end_date: Optional[str] = Query(None, example="2025-12-31"),
):
    """
    order_info 테이블에 적재된 요약 정보 조회 API
    """
    sql = """
        SELECT
            sum(total_event_cnt) AS total_event_cnt,                -- 전체 이벤트 수
            sum(order_cnt) AS order_cnt,
            sum(cancel_cnt) AS cancel_cnt,
            sum(order_cnt) / NULLIF(sum(total_event_cnt), 0) AS order_rate,     -- 주문 비율
            sum(cancel_cnt) / NULLIF(sum(total_event_cnt), 0) AS cancel_rate    -- 취소 비율
        FROM mart_daily_orders
        WHERE (%(start_date)s IS NULL OR order_date >= %(start_date)s::date)
        AND (%(end_date)s IS NULL OR order_date <= %(end_date)s::date)
    """

    rows = fetch_all(sql, {
        "start_date": start_date,
        "end_date": end_date,
    })

    return {
        "data": rows
    }

@app.get("/api/v1/summary/daily-product-sales")
def get_product_sales(
    start_date: Optional[str] = Query(None, example="2025-12-01"),
    end_date: Optional[str] = Query(None, example="2025-12-31"),
    event_type: Optional[str] = Query(None, example="order", regex="^(order|cancel)$"),
    category: Optional[str] = Query(None, example="ETC"),
):
    """
    일별 상품 판매량
        - mart_product_sales 테이블에 적재된 일별 상품 판매량 정보를 조회하는 API
        - event_type (order/cancel)과 category 필터링 기능 제공
    """
    sql = """
        SELECT
            m.stock_code,
            m.category,
            p.product_name,
            CASE WHEN (%(event_type)s = 'order') THEN order_cnt 
                WHEN (%(event_type)s = 'cancel') THEN cancel_cnt
                ELSE order_cnt + cancel_cnt END AS event_cnt,
            CASE WHEN (%(event_type)s = 'order') THEN order_rate 
                WHEN (%(event_type)s = 'cancel') THEN cancel_rate
                ELSE order_rate + cancel_rate END AS event_rate
        FROM mart_product_sales m
        JOIN dim_product p ON m.stock_code = p.stock_code
        WHERE (%(start_date)s IS NULL OR order_date >= %(start_date)s::date)
          AND (%(end_date)s IS NULL OR order_date <= %(end_date)s::date)
          AND (%(category)s IS NULL OR m.category = %(category)s)
        ORDER BY event_cnt DESC
        LIMIT 5
    """

    rows = fetch_all(sql, {
        "start_date": start_date,
        "end_date": end_date,
        "event_type": event_type,
        "category": category,
    })

    return {
        "data": rows,
    }


@app.get("/api/v1/summary/daily-customer-repeats")
def get_customer_repeats(
    start_date: Optional[str] = Query(None, example="2025-12-01"),
    end_date: Optional[str] = Query(None, example="2025-12-31"),
):
    """
    고객별 재구매율 조회 API
     - mart_customer_repeat 테이블에 적재된 일별 고객 재구매율 정보를 조회하는 API
    """
    sql = """
        SELECT
            order_date,
            total_customer_cnt,
            repeat_customer_cnt,
            repeat_customer_rate
        FROM mart_customer_repeat
        WHERE (%(start_date)s IS NULL OR order_date >= %(start_date)s::date) 
          AND (%(end_date)s IS NULL OR order_date <= %(end_date)s::date)
        ORDER BY order_date ASC
    """

    rows = fetch_all(sql, {
        "start_date": start_date,
        "end_date": end_date,
    })

    return {
        "data": rows,
    }

