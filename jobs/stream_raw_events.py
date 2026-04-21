'''
* raw 데이터를 DB에 적재
    - 적재 Table명 : retail_events_raw
* customer/product 데이터 추출
    - customer : dim_customer
    - product : dim_product
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, date_format,
    current_timestamp, lit, coalesce, sha2, concat_ws, trim
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "retail-events"

JDBC_URL = "jdbc:postgresql://postgres:5432/retail_pipeline"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

CHECKPOINT_PATH = "/tmp/checkpoints/retail_events_raw"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("retail-events-raw")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
                "org.postgresql:postgresql:42.7.3"
            ])
        )
        .getOrCreate()
    )


def read_from_kafka(spark, topic):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )


def parse_data(kafka_df):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("invoice_timestamp", StringType(), True)
    ])

    parsed_df = (
        kafka_df
        .selectExpr("CAST(key AS STRING) AS message_key",
                    "CAST(value AS STRING) AS message_value")
        .select(
            col("message_key"),
            from_json(col("message_value"), schema).alias("data")
        )
        .select("message_key", "data.*")
        .withColumn("invoice_timestamp", to_timestamp(col("invoice_timestamp")))
        .withColumn("invoice_date", to_date(col("invoice_timestamp")))
        .withColumn("invoice_time_str", date_format(col("invoice_timestamp"), "HH:mm:ss"))
        .withColumn("description", trim(col("description")))
        .withColumn("ingested_at", current_timestamp())
        .withColumn("load_run_id", lit("sorae"))
        .withColumn(
            "event_id",
            coalesce(
                col("event_id"),
                sha2(
                    concat_ws(
                        "||",
                        coalesce(col("invoice_no"), lit("")),
                        coalesce(col("stock_code"), lit("")),
                        coalesce(col("customer_id"), lit("")),
                        coalesce(col("invoice_timestamp").cast("string"), lit("")),
                        coalesce(col("event_type"), lit(""))
                    ),
                    256
                )
            )
        )
        .select(
            "event_id",
            "event_type",
            "invoice_no",
            "stock_code",
            "description",
            "quantity",
            "unit_price",
            "customer_id",
            "country",
            "invoice_timestamp",
            "invoice_date",
            "invoice_time_str",
            "ingested_at",
            "load_run_id"
        )
    )

    return parsed_df


def write_raw_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    out_df = batch_df.dropDuplicates(["event_id"])

    (
        out_df.write
        .mode("append")
        .option("batchsize", "1000")
        .jdbc(
            url=JDBC_URL,
            table="retail_events_raw",
            properties=JDBC_PROPERTIES
        )
    )


def start_raw_stream(parsed_df):
    return (
        parsed_df.writeStream
        .outputMode("append")
        .foreachBatch(write_raw_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = read_from_kafka(spark, TOPIC)
    parsed_df = parse_data(kafka_df)

    query = start_raw_stream(parsed_df)
    query.awaitTermination()


if __name__ == "__main__":
    main()