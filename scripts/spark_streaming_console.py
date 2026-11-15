
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# =====================================================
# PEDALI BIG DATA 2 - PARTIE 4
# Affichage des evenements Kafka en mode APPEND
# =====================================================

KAFKA_SERVERS = "kafka:19092"
TOPIC = "trips"

CHECKPOINT = "s3a://lake/checkpoints/part4_console_append_v1/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part4-Console-Append")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("start_station", StringType(), True),
    StructField("end_station", StringType(), True),
    StructField("city", StringType(), True),
    StructField("status", StringType(), True),
    StructField("duration_s", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("battery_pct", DoubleType(), True),
    StructField("price_mad", DoubleType(), True),
    StructField("event_time", StringType(), True)
])

print("\n========== PEDALI PARTIE 4 ==========")
print("Lecture Kafka :", TOPIC)
print("Mode de sortie : APPEND")
print("Affichage des nouveaux evenements bruts")

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
    .select(
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("value").cast("string").alias("json_value")
    )
    .withColumn("event", from_json(col("json_value"), schema))
    .select(
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        col("event.trip_id").alias("trip_id"),
        col("event.city").alias("city"),
        col("event.status").alias("status"),
        col("event.vehicle_type").alias("vehicle_type"),
        col("event.event_time").alias("event_time")
    )
)

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 100)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)

print("\nStreaming APPEND demarre !")
print("En attente de nouveaux evenements Kafka...\n")

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
