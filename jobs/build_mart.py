import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct, sum as sum_, round as round_,
    abs as abs_, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, min as min_, max as max_, date_format
from pyspark.sql.types import DecimalType, StringType

JDBC_URL = "jdbc:postgresql://postgres:5432/retail_pipeline"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_mart")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
                "org.postgresql:postgresql:42.7.3"
            ])
        )
        .getOrCreate()
    )

def read_raw(spark):
    df = spark.read.jdbc(
        url=JDBC_URL,
        table="raw_retail_events",
        properties=JDBC_PROPERTIES
    )
    # if start_ts and end_ts:
    #     df = df.filter(
    #         (col("invoice_timestamp") >= lit(start_ts)) &
    #         (col("invoice_timestamp") < lit(end_ts))
    #     )
    return df

def read_customer(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="dim_customer",
        properties=JDBC_PROPERTIES
    )

def read_product(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="dim_product",
        properties=JDBC_PROPERTIES
    )

def prepare_base_df(raw_df):
    """
    mart 계산용 공통 전처리
    - 금액 계산
    - event_type 정리
    - 필요한 컬럼만 유지
    """
    return (
        raw_df
        .filter(col("invoice_date").isNotNull())
        .filter(col("event_type").isin("order", "cancel"))
        .withColumn(
            "sales_amount",
            round_(
                abs_(coalesce(col("quantity"), lit(0)) * coalesce(col("unit_price"), lit(0))),
                2
            )
        )
        .select(
            "event_id",
            "event_type",
            "invoice_no",
            "stock_code",
            "customer_id",
            "invoice_timestamp",
            "invoice_date",
            "quantity",
            "unit_price",
            "sales_amount"
        )
    )

#일별 전체 주문/취소 요약
def build_mart_daily_orders(raw_df):
    daily_df = (
        raw_df
        .groupBy("invoice_date")
        .agg(
            count("*").alias("total_event_cnt"),
            sum_(when(col("event_type") == "order", 1).otherwise(0)).alias("order_cnt"),
            sum_(when(col("event_type") == "cancel", 1).otherwise(0)).alias("cancel_cnt"),
            round_(sum_(when(col("event_type") == "order", col("sales_amount")).otherwise(0)), 2).alias("order_sales_amount"),
            round_(sum_(when(col("event_type") == "cancel", col("sales_amount")).otherwise(0)), 2).alias("cancel_sales_amount")
        )
        .withColumn(
            "total_sales_amount",
            round_(col("order_sales_amount") - col("cancel_sales_amount"), 2)
        )
        .withColumn(
            "order_rate",
            when(col("total_event_cnt") > 0,
                 round_(col("order_cnt") / col("total_event_cnt") * 100, 2))
            .otherwise(lit(0.00))
        )
        .withColumn(
            "cancel_rate",
            when(col("total_event_cnt") > 0,
                 round_(col("cancel_cnt") / col("total_event_cnt") * 100, 2))
            .otherwise(lit(0.00))
        )
        .withColumnRenamed("invoice_date", "order_date")
        .select(
            "order_date",
            "total_event_cnt",
            "order_cnt",
            "cancel_cnt",
            "total_sales_amount",
            "order_sales_amount",
            "cancel_sales_amount",
            "order_rate",
            "cancel_rate"
        )
    )

    return daily_df

# 상품별 판매량/매출 (TODO: 추후 구현)
def build_mart_product_sales(raw_df, dim_product_df):
    """
    일별 상품별 주문/취소 요약
    """
    purchase_df = (
        raw_df
        .join(dim_product_df, on="stock_code", how="left")
        .groupby("stock_code", "invoice_date", "category")
        .agg(
            sum_(col("quantity")).alias("total_event_cnt"),
            sum_(when(col("event_type") == "order", col("quantity")).otherwise(0)).alias("order_cnt"),
            sum_(when(col("event_type") == "cancel", col("quantity")).otherwise(0)).alias("cancel_cnt"),
            sum_(when(col("event_type") == "order", col("sales_amount")).otherwise(0)).alias("order_sales_amount"),
            sum_(when(col("event_type") == "cancel", col("sales_amount")).otherwise(0)).alias("cancel_sales_amount")
        )
        # .withColumn("category", col("category"))
        .withColumn(
            "order_rate",
            when(col("total_event_cnt") > 0,
                 round_(col("order_cnt") / col("total_event_cnt") * 100, 2))
            .otherwise(lit(0.00))
        )
        .withColumn(
            "cancel_rate",
            when(col("total_event_cnt") > 0,
                 round_(col("cancel_cnt") / col("total_event_cnt") * 100, 2))
            .otherwise(lit(0.00))
        )
        .withColumnRenamed("invoice_date", "order_date")
        .select(
            "stock_code",
            "order_date",
            "category",
            "total_event_cnt",
            "order_cnt",
            "cancel_cnt",
            "order_sales_amount",
            "cancel_sales_amount",
            "order_rate",
            "cancel_rate"
        )
    )

    return purchase_df

# 고객 재구매율 (날짜별 누적)
def build_mart_customer_repeat(raw_df, dim_customer_df):
    """
    고객 재구매율 (날짜별 누적)
    정의:
    - 각 날짜별로, 그 날짜까지 구매한 고객 중
    - 누적 주문 건수(distinct invoice_no)가 2건 이상인 고객 비율
    """
    # Step 1: 고객별 누적 주문 건수 계산
    customer_repeat_daily = (
        raw_df
        .join(dim_customer_df.select("customer_id", "total_order_count"), on="customer_id", how="inner")
        .filter(col("event_type") == "order")
        # .select("customer_id", "invoice_date", "total_order_count")
        # .distinct()
        .withColumn(
            "is_repeat",
            when(col("total_order_count") >= 2, 1).otherwise(0)
        )
        .select("invoice_date", "customer_id", "is_repeat")
        .distinct()
    )
    
    # Step 2: 날짜별로 전체 고객 수와 재구매 고객 수 집계
    customer_purchase_df = (
        customer_repeat_daily
        .groupBy("invoice_date")
        .agg(
            countDistinct(col("customer_id")).alias("total_customer_cnt"),
            sum_(col("is_repeat")).alias("repeat_customer_cnt"),
        )
        .withColumn(
            "repeat_customer_rate",
            when(col("total_customer_cnt") > 0,
                  round_(col("repeat_customer_cnt") / col("total_customer_cnt") * 100, 2))
            .otherwise(lit(0.00))
        )
        .withColumnRenamed("invoice_date", "order_date")
        .select(
            "order_date",
            "total_customer_cnt", 
            "repeat_customer_cnt", 
            "repeat_customer_rate"
        )
    )

    return customer_purchase_df


def write_table(df, table_name, mode="append"):
    (
        df.write
        .mode(mode)
        .option("batchsize", "1000")
        .jdbc(
            url=JDBC_URL,
            table=table_name,
            properties=JDBC_PROPERTIES
        )
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    return parser.parse_args()


def main():
    # args = parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw(spark) #, start_ts=args.start, end_ts=args.end)
    dim_product_df = read_product(spark)
    dim_customer_df = read_customer(spark)

    base_df = prepare_base_df(raw_df)

    mart_daily_orders_df = build_mart_daily_orders(base_df)
    mart_product_sales_df = build_mart_product_sales(base_df, dim_product_df)
    mart_customer_repeat_df = build_mart_customer_repeat(base_df, dim_customer_df)

    write_table(mart_daily_orders_df, "mart_daily_orders", mode="overwrite")
    write_table(mart_product_sales_df, "mart_product_sales", mode="overwrite")
    write_table(mart_customer_repeat_df, "mart_customer_repeat", mode="overwrite")  # 매번 전체 갱신


if __name__ == "__main__":
    main()