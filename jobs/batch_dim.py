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
        .appName("batch_dim")
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


def build_dim_customer(raw_df):
    order_df = raw_df.filter(
        (col("event_type") == "order") &
        col("customer_id").isNotNull()
    )

    return (
        order_df.groupBy("customer_id")
        .agg(
            min_("invoice_timestamp").alias("first_purchase_at"),
            max_("invoice_timestamp").alias("last_purchase_at"),
            countDistinct("invoice_no").alias("total_order_count")
        )
    )


def build_dim_product(raw_df):
    order_df = raw_df.filter(
        (col("event_type") == "order") &
        col("stock_code").isNotNull()
    )

    base_df = (
        order_df.groupBy("stock_code")
        .agg(
            first("description", ignorenulls=True).alias("description"),
            max_("unit_price").alias("latest_unit_price")
        )
    )

    return (
        base_df
        .withColumn("proc_type", split(coalesce(col("description"), lit("")), r"\s+").getItem(0))
        .withColumn("product_name", col("description"))
        .withColumn("category", lit(None).cast("string"))
        .select(
            "stock_code",
            "category",
            "description",
            "proc_type",
            "product_name",
            "latest_unit_price"
        )
    )


def write_dim(df, table_name):
    (
        df.write
        .mode("overwrite")
        .option("truncate", "true")
        .jdbc(
            url=JDBC_URL,
            table=table_name,
            properties=JDBC_PROPERTIES
        )
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw(spark)

    dim_customer_df = build_dim_customer(raw_df)
    dim_product_df = build_dim_product(raw_df)

    write_dim(dim_customer_df, "dim_customer")
    write_dim(dim_product_df, "dim_product")


if __name__ == "__main__":
    main()