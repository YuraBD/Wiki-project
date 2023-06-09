from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, count, lit, desc, col, window, current_timestamp, collect_set, hour, to_utc_timestamp, date_trunc
from pyspark.sql.window import Window
import time
import schedule

def my_func():
    spark = SparkSession.builder \
        .appName("WikipediaHourlyStats") \
        .getOrCreate()

    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="raw_data_table", keyspace="wikimedia") \
        .load()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    df = df.where((col("timestamp") >= date_trunc('hour', current_timestamp()) - expr('INTERVAL 8 HOURS')) & 
                (col("timestamp") < date_trunc('hour', current_timestamp())))

    df.groupBy(window("timestamp", "1 hour"), "domain") \
        .count() \
        .selectExpr("window.start as time_start", "window.end as time_end", "domain", "count as page_count") \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="domain_page_create_stats", keyspace="wikimedia") \
        .option("confirm.truncate","true") \
        .mode("overwrite") \
        .save()

    df.where(col("user_is_bot")) \
        .groupBy(window("timestamp", "1 hour"), "domain") \
        .count() \
        .selectExpr("window.start as time_start", "window.end as time_end", "domain", "count as bot_page_count") \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="bot_page_create_stats", keyspace="wikimedia") \
        .option("confirm.truncate","true") \
        .mode("overwrite") \
        .save()

    df.groupBy(window("timestamp", "1 hour"), "user_text", "user_id") \
        .agg(count("page_id").alias("page_count"), collect_set("page_title").alias("page_titles")) \
        .orderBy(desc("page_count")) \
        .selectExpr("window.start as time_start", "window.end as time_end", "user_id", "user_text as user_name", "page_titles", "page_count") \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="user_page_create_stats", keyspace="wikimedia") \
        .option("confirm.truncate","true") \
        .mode("overwrite") \
        .save()

schedule.every(1).hours.do(my_func)

while True:
    schedule.run_pending()
    time.sleep(1)
