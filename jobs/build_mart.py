from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, count, countDistinct, sum as sum_, round as round_,
    abs as abs_, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, min as min_, max as max_, date_format
from pyspark.sql.types import DecimalType

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
            "org.postgresql:postgresql:42.7.3"
        )
        .getOrCreate()
    )

def read_raw(spark):
    return spark.read.jdbc(
        url=JDBC_URL,
        table="retail_events_raw",
        properties=JDBC_PROPERTIES
    )

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

# 상품별 구매율
def build_mart_product_sales(raw_df) :
    """
    일별 상품별 주문/취소 요약
    """
    return raw_df

# 고객 재구매율
def build_mart_customer_repeat(raw_df) :
    """
    고객 재구매율
    정의:
    - 특정 날짜(order_date)에 구매한 고객 중
    - 그 날짜까지 누적 주문 건수(distinct invoice_no)가 2건 이상인 고객 비율
    """
    return raw_df


def write_table(df, table_name):
    (
        df.write
        .mode("overwrite")
        .option("batchsize", "1000")
        .jdbc(
            url=JDBC_URL,
            table=table_name,
            properties=JDBC_PROPERTIES
        )
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_table(spark, "retail_events_raw")
    dim_product_df = read_table(spark, "dim_product")

    base_df = prepare_base_df(raw_df)

    mart_daily_orders_df = build_mart_daily_orders(base_df)
#    mart_product_sales_df = build_mart_product_sales(base_df, dim_product_df)
#    mart_customer_repeat_df = build_mart_customer_repeat(base_df)

    write_table(mart_daily_orders_df, "mart_daily_orders")
    # write_table(mart_product_sales_df, "mart_product_sales")
    # write_table(mart_customer_repeat_df, "mart_customer_repeat")


if __name__ == "__main__":
    main()