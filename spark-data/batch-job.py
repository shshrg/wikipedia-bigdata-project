from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

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

hourly_activity = (
    df
    .withColumn(
        "hour",
        F.date_trunc("hour", F.col("dt"))
    )
    .groupBy(
        "hour",
        "domain"
    )
    .agg(
        F.count("*").alias("pages_created"),
        F.approx_count_distinct(F.col("user_name")).alias("unique_authors"),
        F.round(
            F.count(F.when(F.col("user_is_bot")==True, True)) /
            F.count(F.when(F.col("user_is_bot")==False, True)),
            2
        ).alias("bot_human_ratio"),
        F.slice(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        F.col("user_name").alias("name"),
                        F.col("user_is_bot").alias("is_bot")
                    )
                ),
                lambda x, y: F.when(x["name"] < y["name"], -1)
                              .when(x["name"] > y["name"], 1)
                              .otherwise(0)
            ),
            1, 10
        ).alias("top_authors")
    )
)

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
