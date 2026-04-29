'''
* raw 데이터를 DB에 적재
    - 적재 Table명 : raw_retail_events
* customer/product 데이터 추출
    - customer : dim_customer
    - product : dim_product
'''
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, to_date, date_format,
    current_timestamp, lit, coalesce, sha2, concat_ws, trim,
    first
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

CHECKPOINT_PATH = "/tmp/checkpoints/raw_retail_events"


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

# tag를 기준으로 Streaming 배치 내에서 데이터를 분리하여 처리 주문 완료/미완료 분기 처리
def branch_by_tag(batch_df):
    raw_df = batch_df.filter(col('tag') != 'complete')
    complete_invoice_df = (
        batch_df
        .filter(col('tag') == 'complete')
        .select('invoice_no')
        .distinct()
    )
    return raw_df, complete_invoice_df

# raw 데이터 적재
def parse_data(kafka_df):
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("original_invoice_timestamp", StringType(), True),
        StructField("invoice_timestamp", StringType(), True),
        StructField("target_date", StringType(), True),
        StructField("target_time", StringType(), True),
        StructField("tag", StringType(), True)
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
        .withColumn("org_invoice_timestamp", to_timestamp("original_invoice_timestamp")) \
        .withColumn("invoice_timestamp", to_timestamp(col("invoice_timestamp")))
        .withColumn("invoice_date", to_date(col("invoice_timestamp")))
        .withColumn("invoice_time", date_format(col("invoice_timestamp"), "HH:mm:ss"))
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
                        coalesce(col("category"), lit("")),
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
            "category",
            "description",
            "quantity",
            "unit_price",
            "customer_id",
            "country",
            "invoice_timestamp",
            "invoice_date",
            "invoice_time",
            "org_invoice_timestamp",
            "ingested_at",
            "load_run_id",
            "tag"
        )
    )

    return parsed_df

# raw_retail_events를 invoice_no 기준으로 추출
def read_orders(spark, invoice_no):
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option(
            "query",
            f"SELECT * FROM raw_retail_events WHERE invoice_no = '{invoice_no}'"
        )
        .option("user", JDBC_PROPERTIES["user"])
        .option("password", JDBC_PROPERTIES["password"])
        .option("driver", JDBC_PROPERTIES["driver"])
        .load()
    )

# invoice_no 기준으로 주문 정보 추출하여 order_info 테이블에 적재
def make_order_info(batch_df):
    """
        master 데이터 merge/update하여 order_info 테이블에 적재
        - invoice_no : group by 기준
        - event_type : order/cancel
        - invoice_timestamp
        - invoice_date
        - invoice_time
        - customer_id
        - country
    """

    order_info_df = (
        batch_df.groupby('invoice_no')
            .agg(
                first('event_type', ignorenulls=True).alias('event_type'),
                first('invoice_timestamp').alias('invoice_timestamp'),
                first('invoice_date').alias('invoice_date'),
                first('invoice_time').alias('invoice_time'),
                first('customer_id').alias('customer_id'),
                first('country').alias('country')
            )
    )
    return order_info_df, 'order_info'

# raw_retail_events에서 상품 정보 추출하여 order_detail 테이블에 적재
def make_order_detail(batch_df):
    """
        master 데이터 merge/update하여 order_detail 테이블에 적재
        - event_id : unique key
        - event_type : order/cancel
        - invoice_no
        - stock_code
        - category
        - description
        - quantity
        - unit_price
    """

    order_detail_df = (
        batch_df.select(
            "event_id",
            "event_type",
            "invoice_no",
            "stock_code",
            "category",
            "description",
            "quantity",
            "unit_price"
        )
    )
    return order_detail_df, 'order_detail'

# raw_retail_events 테이블에 적재된 데이터를 invoice_no 기준으로 
# 주문 완료/미완료 분기 처리하여 order_info, order_detail 테이블에 적재
def write_raw_batch(batch_df, batch_id):
    print(f"\n=== batch_id: {batch_id} ===")
    batch_df.groupBy("tag").count().show(truncate=False)

    if batch_df.rdd.isEmpty():
        return

    raw_df, complete_invoice_df = branch_by_tag(batch_df)

    print("raw empty:", raw_df.rdd.isEmpty())
    print("complete empty:", complete_invoice_df.rdd.isEmpty())

    if not raw_df.rdd.isEmpty():
        out_df = raw_df.dropDuplicates(["event_id"])
        (
            out_df.write
            .mode("append")
            .option("batchsize", "1000")
            .jdbc(
                url=JDBC_URL,
                table="raw_retail_events",
                properties=JDBC_PROPERTIES
            )
        )

    if not complete_invoice_df.rdd.isEmpty():
        spark = batch_df.sparkSession
        invoice_nos = [row.invoice_no for row in complete_invoice_df.collect()]
        print("complete invoice_nos:", invoice_nos)
        for invoice_no in invoice_nos:
            order_batch_df = read_orders(spark, invoice_no)
            order_info_df, _ = make_order_info(order_batch_df)
            order_detail_df, _ = make_order_detail(order_batch_df)
            print("order_info count:", order_info_df.count())
            print("order_detail count:", order_detail_df.count())


            (
                order_info_df.write
                .mode("append")
                .option("batchsize", "1000")
                .jdbc(
                    url=JDBC_URL,
                    table="order_info",
                    properties=JDBC_PROPERTIES
                )
            )

            (
                order_detail_df.write
                .mode("append")
                .option("batchsize", "1000")
                .jdbc(
                    url=JDBC_URL,
                    table="order_detail",
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