from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import datetime

spark = (
    SparkSession.builder
    .appName("wikipedia-historical-analysis")
    .config("spark.cassandra.connection.host", "wiki-cassandra")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()
)

df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="page_events", keyspace="wikipedia_analytics") \
    .load()

# B1. Hourly Activity Report

now = datetime.datetime.now(datetime.timezone.utc)
current_hour_start = now.replace(minute=0, second=0, microsecond=0)
six_hours_ago = current_hour_start - datetime.timedelta(hours=6)

df_recent = df.filter(
    (F.col("dt") >= six_hours_ago) & 
    (F.col("dt") < current_hour_start)
)

df_hourly = df_recent.withColumn("hour", F.date_trunc("hour", F.col("dt")))

hourly_stats = (
    df_hourly
    .groupBy("hour", "domain")
    .agg(
        F.count("*").alias("pages_created"),
        F.approx_count_distinct("user_name").alias("unique_authors"),
        F.round(
            F.sum(F.when(F.col("user_is_bot") == True, 1).otherwise(0)) / 
            F.when(F.sum(F.when(F.col("user_is_bot") == False, 1).otherwise(0)) == 0, 1) # Prevent divide by zero
            .otherwise(F.sum(F.when(F.col("user_is_bot") == False, 1).otherwise(0))),
            2
        ).alias("bot_human_ratio")
    )
)

user_counts = (
    df_hourly
    .groupBy("hour", "domain", "user_name", "user_is_bot")
    .agg(F.count("*").alias("pages_by_user"))
)

window_top_users = Window.partitionBy("hour", "domain").orderBy(F.col("pages_by_user").desc())

top_users_array = (
    user_counts
    .withColumn("rank", F.row_number().over(window_top_users))
    .filter(F.col("rank") <= 10)
    .groupBy("hour", "domain")
    .agg(
        F.collect_list(
            F.struct(
                F.col("user_name").alias("name"),
                F.col("pages_by_user").alias("pages"),
                F.col("user_is_bot").alias("is_bot")
            )
        ).alias("top_authors")
    )
)

hourly_activity = hourly_stats.join(top_users_array, ["hour", "domain"], "left")

hourly_activity.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="hourly_activity", keyspace="wikipedia_analytics") \
    .mode("append") \
    .save()

# B2. Editor Behavior Patterns

user_window = Window.partitionBy("user_name").orderBy("dt")

editor_patterns = (
    df
    .withColumn(
        "prev_dt",
        F.lag("dt").over(user_window)
    )
    .withColumn(
        "time_diff",
        F.col("dt").cast("long") - F.col("prev_dt").cast("long")
    )
    .groupBy("user_name")
    .agg(
        F.count("*").alias("total_pages"),
        F.avg("time_diff").alias("avg_gap_seconds"),
        F.approx_count_distinct("domain").alias("unique_domains"),
        F.collect_set(F.hour("dt")).alias("activity_hours")
    )
    .filter(F.col("total_pages") >= 5)
)


domain_counts = (
    df
    .groupBy("user_name", "domain")
    .agg(
        F.count("*").alias("domain_count")
    )
)

specialization_w = Window.partitionBy("user_name").orderBy(F.col("domain_count").desc())

specialization = (
    domain_counts
    .withColumn(
        "row_num",
        F.row_number().over(specialization_w)
    )
    .filter(
        F.col("row_num") == 1
    )
    .select(
        "user_name",
        F.col("domain").alias("specialization")
    )
)

editor_patterns_with_spec = (
    editor_patterns
    .join(
        specialization,
        "user_name",
        "left"
    )
)

editor_patterns_with_spec.write \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="editor_behavior_patterns", keyspace="wikipedia_analytics") \
    .mode("append") \
    .save()

spark.stop()
