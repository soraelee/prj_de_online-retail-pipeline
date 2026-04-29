import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as min_, max as max_, countDistinct,
    first, split, lit, coalesce
)

JDBC_URL = "jdbc:postgresql://postgres:5432/retail_pipeline"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("build_dim")
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

# 고객 데이터 생성
def build_dim_customer(raw_df):
    order_df = raw_df.filter(
        (col("event_type") == "order") &
        col("customer_id").isNotNull()          # 고객ID 없는 데이터는 dim_customer에 적재하지 않음
    )

    return (
        order_df.groupBy("customer_id")
        .agg(
            min_("invoice_timestamp").alias("first_purchase_at"),
            max_("invoice_timestamp").alias("last_purchase_at"),
            countDistinct("invoice_no").alias("total_order_count"),
            first("country", ignorenulls=True).alias("country"),
        )
    )

#상품 데이터 생성
def build_dim_product(raw_df):
    order_df = raw_df.filter(
        (col("event_type") == "order") &
        col("stock_code").isNotNull()
    )

    base_df = (
        order_df.groupBy("stock_code")
        .agg(
            first("description", ignorenulls=True).alias("description"),
            first("category", ignorenulls=True).alias("category"),
            max_("unit_price").alias("latest_unit_price")
        )
    )

    return (
        base_df
        .withColumn("product_name", col("description"))
        .select(
            "stock_code",
            "category",
            "description",
            "product_name",
            "latest_unit_price"
        )
    )


def write_dim(df, table_name):
    (
        df.write
        .mode("append")
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

    raw_df = read_raw(spark)

    dim_customer_df = build_dim_customer(raw_df)
    dim_product_df = build_dim_product(raw_df)

    write_dim(dim_customer_df, "dim_customer")
    write_dim(dim_product_df, "dim_product")
    
    print("dim_customer, dim_product 생성 완료")


if __name__ == "__main__":
    main()