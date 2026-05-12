from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, BooleanType
import pandas as pd

KAFKA_URL = "wiki-kafka:9092"

spark = (
    SparkSession.builder.appName("new-pages-streaming")
    .config("spark.cassandra.connection.host", "wiki-cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)


schema = StructType() \
    .add("page_title", StringType()) \
    .add("user_name", StringType()) \
    .add("user_is_bot", BooleanType()) \
    .add("user_edit_count", IntegerType()) \
    .add("domain", StringType()) \
    .add("dt", TimestampType())


df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("subscribe", "new-pages")
    .load()
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), schema).alias("data"))
    .select("data.*")
)

# A1. Breaking News Detection

# a. Statistic Spike Detection
pages_count = (
    df
    .withWatermark("dt", "1 hour")
    .groupBy(
        F.window("dt", "5 minutes"),
        F.col("domain")
    )
    .agg(
        F.count("*").alias("new_page_count")
    )
)

activity_spike = (
    pages_count
    .groupBy(
        F.window("window.start", "1 hour", "5 minutes"),
        F.col("domain")
    )
    .agg(
        F.avg("new_page_count").alias("avg_pages_per_5min"),
        F.max("new_page_count").alias("pages_last_5min")
    )
    .withColumn("spike_ratio", F.col("pages_last_5min") / F.col("avg_pages_per_5min"))
    .filter(F.col("spike_ratio") >= 3.0)
    .withColumn("alert_type", "activity_spike")
)

activity_spike_sink = (
    activity_spike.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "breaking-news-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a1_0")
    .start()
)

# b. Keyword Burst Detection

word_frequencies = (
    df
    .withWatermark("1 minute")
    .withColumn("tokens", F.split(F.lower(F.col("page_title")), " "))
    .withColumn("word", F.explode(F.col("tokens")))
    .filter(
        ~F.col("word").isin(["a", "an", "the", "file", "and", "of", "in", "out", "on"]) &
        (F.length(F.col("word")) > 2)
    )
    .groupBy(
        F.window("dt", "10 minutes"),
        F.col("word")
    )
    .agg(F.count("*").alias("occurences"))
    .filter(F.col("occurences") >= 5)
)

keyboard_burst_sink = (
    word_frequencies.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "breaking-news-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a1_1")
    .start()
)

# A2. Bot vs Human activity monitor

# 1. Write all info about activity into Cassandra
bot_activity = (
    df
    .withWatermark("1 minute")
    .groupBy(
        F.window("dt", "1 minute")
    )
    .agg(
        F.count(F.when(F.col("user_is_bot") == True, True)).alias("bot_count"),
        F.count(F.when(F.col("user_is_bot") != True, True)).alias("human_count")
    )
    .withColumn(
        "bot_activity",
        F.round(F.col("bot_count") * 100 / (F.col("human_count") + F.col("bot_count")), 2)
    )
    .withColumn(
        "bot_activity_percentage",
        F.concat(F.col("bot_activity").cast("string"),
                 F.lit("%"))
    )
)

bot_activity_cassandra_sink = (
    bot_activity
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "bot_count",
        "human_count",
        "bot_activity_percentage"
    )
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .options(table="bot_activity_metrics", keyspace="wikipedia_analytics")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a2_0")
    .outputMode("append")
    .start()
)

# 2. Alert if bot_activity > 0.8

bot_activity_kafka_alert = (
    bot_activity
    .filter(F.col("bot_activity") > 0.8)
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "bot-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a2_1")
    .start()
)

# 3. Alert if 1 bot has created more than 50 pages in the last 10 mins
bot_activity_10mins = (
    df
    .filter(
        F.col("user_is_bot") == True
    )
    .withWatermark("1 minute")
    .groupBy(
        F.window("dt", "10 minutes", "1 minute"),
        F.col("user_name")
    )
    .agg(
        F.count("*").alias("bot_pages")
    )
    .filter(F.col("bot_pages") > 50)
)

bot_activity_10mins_kafka_alert = (
    bot_activity_10mins
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "bot-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a2_2")
    .start()
)

# A3. Language Activity Dashboard

current_language_activity = (
    df
    .withWatermark("dt", "2 minutes")
    .groupBy(
        F.window("dt", "1 minute"),
        "domain"
    )
    .agg(
        F.count("*").alias("new_page_count"),
        F.approx_count_distinct(F.col("user_name")).alias("unique_authors"),
        F.avg(F.length(F.col("page_title"))).alias("average_title_length")
    )
    .select(
        F.col("window.start").alias("current_window_start"),
        "domain",
        "new_page_count",
        "unique_authors",
        "average_title_length"
    )
)

prev_language_activity = (
    current_language_activity
    .withColumn(
        "next_window_start",
        F.col("current_window_start") + F.expr("INTERVAL 1 MINUTE")
    )
    .select(
        "next_window_start",
        F.col("domain").alias("prev_domain"),
        F.col("new_page_count").alias("prev_count")
    )
)

language_activity = (
    current_language_activity
    .join(
        prev_language_activity,
        (F.col("current_window_start") == F.col("next_window_start")) &
        (F.col("domain") == F.col("prev_domain")),
        "left"
    )
    .withColumn(
        "trend",
        F.round(F.col("new_page_count") - F.fillna(F.col("prev_count"), 0), 2)
    )
)


language_activity_cassandra_sink = (
    language_activity
    .select(
        F.col("current_window_start").alias("window_start"),
        F.col("domain"),
        F.col("new_page_count"),
        F.col("unique_titles"),
        F.col("average_title_length")
    )
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .options(table="language_activity", keyspace="wikipedia_analytics")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a3")
    .outputMode("append")
    .start()
)

# A4. Spam & Vandalism Detector

