
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count
)
from pyspark.sql.types import StructType, StructField, StringType

# ==========================================================
# PEDALI BIG DATA 2 - PARTIE 4
# Test watermark 10 minutes et evenements en retard
# ==========================================================

KAFKA_SERVERS = "kafka:19092"
TOPIC = "trips"

CHECKPOINT = "s3a://lake/checkpoints/part4_watermark_v1/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part4-Watermark")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("event_time", StringType(), True)
])

print("\n========== PEDALI - WATERMARK ==========")
print("Topic :", TOPIC)
print("Watermark : 10 minutes")
print("Fenetre : 5 minutes")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

events = (
    kafka_stream
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("event"))
    .select("event.*")
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .filter(
        col("event_ts").isNotNull()
        & col("city").isNotNull()
        & col("status").isNotNull()
    )
)

counts = (
    events
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        window(col("event_ts"), "5 minutes"),
        col("city"),
        col("status")
    )
    .agg(count("*").alias("total_events"))
)

query = (
    counts.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("numRows", 100)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)

print("\nStreaming watermark demarre !")
print("En attente des evenements Kafka...\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nInterruption demandee.")
finally:
    try:
        if query.isActive:
            query.stop()
    except Exception:
        pass
    try:
        spark.stop()
    except Exception:
        pass
