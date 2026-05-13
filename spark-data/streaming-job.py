from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, BooleanType

KAFKA_URL = "wiki-kafka:9092"

spark = (
    SparkSession.builder.appName("new-pages-streaming")
    .config("spark.cassandra.connection.host", "wiki-cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")

schema = StructType() \
    .add("page_title", StringType()) \
    .add("page_id", StringType()) \
    .add("user_name", StringType()) \
    .add("user_is_bot", BooleanType()) \
    .add("user_edit_count", IntegerType()) \
    .add("domain", StringType()) \
    .add("dt", TimestampType())


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_URL) \
    .option("subscribe", "new-pages") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*")


all_events_cassandra_sink = (
    df
    .drop("user_edit_count")
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .options(table="page_events", keyspace="wikipedia_analytics")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a0")
    .outputMode("append")
    .start()
)

# Tables used by REST API
pages_by_id_sink = (
    df
    .select("page_id", "page_title", "domain", "user_name", "user_is_bot", "dt")
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .options(table="pages_by_id", keyspace="wikipedia_analytics")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a0_pages_by_id")
    .outputMode("append")
    .start()
)


pages_by_domain_sink = (
    df
    .select("domain", "dt", "page_id", "page_title", "user_name", "user_is_bot")
    .writeStream
    .format("org.apache.spark.sql.cassandra")
    .options(table="pages_by_domain", keyspace="wikipedia_analytics")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a0_pages_by_domain")
    .outputMode("append")
    .start()
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
        F.count("*").alias("new_page_count"),
        F.slice(F.collect_list("page_title"), 1, 2).alias("sample_pages")
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
        F.max("new_page_count").alias("pages_last_5min"),
        F.last("sample_pages").alias("sample_pages")
    )
    .withColumn("spike_ratio", F.col("pages_last_5min") / F.col("avg_pages_per_5min"))
    .filter(F.col("spike_ratio") >= 3.0)
    .select(
        F.current_timestamp().cast("string").alias("alert_time"),
        F.lit("activity_spike").alias("alert_type"),
        "domain",
        "pages_last_5min",
        F.round("avg_pages_per_5min", 2).alias("avg_pages_per_5min"),
        "spike_ratio",
        "sample_pages"
    )
)

activity_spike_sink = (
    activity_spike
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
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
    .withWatermark("dt", "10 minutes")
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
    .agg(
        F.count("*").alias("occurrences"),
        F.collect_set("domain").alias("domains"),
        F.slice(F.collect_list("page_title"), 1, 3).alias("sample_pages")
    )
    .filter(F.col("occurrences") >= 5)
    .select(
        F.current_timestamp().cast("string").alias("alert_time"),
        F.lit("keyword_burst").alias("alert_type"),
        F.col("word").alias("keyword"),
        "occurrences",
        "domains",
        "sample_pages"
    )
)

keyboard_burst_sink = (
    word_frequencies
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
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
    .withWatermark("dt", "1 minute")
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
    .selectExpr("to_json(struct(*)) AS value")
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
    .withWatermark("dt", "10 minutes")
    .filter(
        F.col("user_is_bot") == True
    )
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
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "bot-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a2_2")
    .start()
)

# A3. Language Activity Dashboard

language_activity = (
    df
    .withWatermark("dt", "2 minutes")
    .groupBy(
        F.window("dt", "1 minute"),
        "domain"
    )
    .agg(
        F.count("*").alias("new_page_count"),
        F.approx_count_distinct(F.col("user_name")).alias("unique_authors"),
        F.round(F.avg(F.length(F.col("page_title"))), 2).alias("average_title_length")
    )
    .select(
        F.col("window.start").alias("window_start"),
        "domain",
        "new_page_count",
        "unique_authors",
        "average_title_length"
    )
)

def find_trend_and_write(batch_df, batch_id):
    domain_window = Window.partitionBy("domain").orderBy("window_start")
    batch_with_trend = (
        batch_df
        .withColumn(
            "prev_count",
            F.lag("new_page_count", 1).over(domain_window)
        )
        .withColumn(
            "trend",
            F.round(F.col("new_page_count") - F.col("prev_count"), 2)
        )
        .fillna(0, subset=["trend"])
        .drop("prev_count")
    )
    batch_with_trend.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="language_activity", keyspace="wikipedia_analytics") \
        .mode("append") \
        .save()


language_activity_cassandra_sink = (
    language_activity
    .writeStream
    .foreachBatch(find_trend_and_write)
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a3")
    .outputMode("update")
    .start()
)

# A4. Spam & Vandalism Detector

# 1. Detect users who create > 10 pages in 5 minutes and/or new users who create pages in different languages

suspicious_users = (
    df
    .withWatermark("dt", "2 minutes")
    .groupBy(
        F.window("dt", "5 minutes", "1 minute"),
        "user_name",
        "user_is_bot"
    )
    .agg(
        F.count("*").alias("total_pages_created"),
        F.approx_count_distinct(F.col("domain")).alias("unique_domains_count"),
        F.max(F.col("user_edit_count")).alias("user_edit_count")
    )
)

url_pattern = r"https?://\S+"
phone_pattern = r"\+?\d{1,4}?[-.\s]?\(?\d{1,3}?\)?[-.\s]?\d{1,4}[-.\s]?\d{1,4}"
excessive_numbers = r"(\d.*){7,}"

suspicious_titles = (
    df
    .withWatermark("dt", "2 minutes")

)


# low severity: short/long page titles
low_severity_kafka_alert = (
    suspicious_titles
    .filter(
        (F.length(F.col("page_title")) <= 3) | (F.length(F.col("page_title"))>= 40)
    )
    .withColumn("severity", F.lit("low"))
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "spam-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a4_0")
    .start()
)

# medium severity: suspicious titles
medium_severity_kafka_alert = (
    suspicious_titles
    .filter(
        F.col("page_title").rlike(url_pattern) | 
        F.col("page_title").rlike(phone_pattern) | 
        F.col("page_title").rlike(excessive_numbers)
    )
    .withColumn("severity", F.lit("medium"))
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "spam-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a4_1")
    .start()
)

# high severity: suspicious user activity
high_severity_kafka_alert = (
    suspicious_users
    .filter(
        ((F.col("total_pages_created") > 10) & (F.col("user_is_bot") == False)) |
        ((F.col("unique_domains_count") > 1) & (F.col("user_edit_count") <= F.col("total_pages_created")))
    )
    .withColumn("severity", F.lit("high"))
    .selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_URL)
    .option("topic", "spam-alerts")
    .outputMode("update")
    .option("checkpointLocation", "/opt/spark-data/checkpoints/a4_2")
    .start()
)

spark.streams.awaitAnyTermination()