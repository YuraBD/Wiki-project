from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, lit, create_map
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ArrayType
from itertools import chain


spark = SparkSession.builder \
    .appName("WikipediaStream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

schema = StructType([
    StructField("$schema", StringType()),
    StructField("meta", StructType([
        StructField("uri", StringType()),
        StructField("request_id", StringType()),
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
        StructField("stream", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", IntegerType())
    ])),
    StructField("database", StringType()),
    StructField("page_id", IntegerType()),
    StructField("page_title", StringType()),
    StructField("page_namespace", IntegerType()),
    StructField("rev_id", IntegerType()),
    StructField("rev_timestamp", StringType()),
    StructField("rev_sha1", StringType()),
    StructField("rev_minor_edit", BooleanType()),
    StructField("rev_len", IntegerType()),
    StructField("rev_content_model", StringType()),
    StructField("rev_content_format", StringType()),
    StructField("performer", StructType([
        StructField("user_text", StringType()),
        StructField("user_groups", ArrayType(StringType())),
        StructField("user_is_bot", BooleanType()),
        StructField("user_id", IntegerType()),
        StructField("user_registration_dt", StringType()),
        StructField("user_edit_count", IntegerType())
    ])),
    StructField("page_is_redirect", BooleanType()),
    StructField("comment", StringType()),
    StructField("parsedcomment", StringType()),
    StructField("rev_slots", StructType([
        StructField("main", StructType([
            StructField("rev_slot_content_model", StringType()),
            StructField("rev_slot_sha1", StringType()),
            StructField("rev_slot_size", IntegerType()),
            StructField("rev_slot_origin_rev_id", IntegerType())
        ]))
    ]))
])

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "wikipedia") \
  .load() 

df = df.selectExpr("CAST(value AS STRING)") \
  .select(from_json(col("value").cast("string"), schema).alias("data"))

df = df.select(
    col("data.page_id"),
    to_timestamp("data.meta.dt", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp"),
    col("data.meta.domain"),
    col("data.performer.user_is_bot"),
    col("data.performer.user_text"),
    col("data.performer.user_id"),
    col("data.page_title")
)

df = df.na.drop(how="any")

query = df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "wikimedia") \
    .option("table", "raw_data_table") \
    .option("checkpointLocation", "~/UCU/BD/checkpoint") \
    .start()

query.awaitTermination()
