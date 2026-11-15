
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# ============================================================
# PEDALI BIG DATA 2 - PARTIE 4
# Kafka -> Spark Structured Streaming -> MinIO Bronze
# ============================================================

KAFKA_SERVERS = "kafka:19092"
KAFKA_TOPIC = "trips"

BRONZE_PATH = "s3a://lake/bronze/trips_raw/"
CHECKPOINT_PATH = "s3a://lake/checkpoints/part4_bronze/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part4-Kafka-Streaming")
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

print("\n========== PEDALI PARTIE 4 ==========")
print("Spark version :", spark.version)
print("Kafka topic :", KAFKA_TOPIC)
print("Destination Bronze :", BRONZE_PATH)

# Schéma explicite des événements Pedali
event_schema = StructType([
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
    StructField("event_time", StringType(), True),
])

# 1. Lire le topic Kafka en streaming
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# 2. Convertir les messages Kafka JSON en colonnes
events = (
    kafka_stream
    .selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "timestamp AS kafka_timestamp",
        "partition AS kafka_partition",
        "offset AS kafka_offset"
    )
    .withColumn("event", from_json(col("raw_json"), event_schema))
    .select(
        "raw_json",
        "kafka_timestamp",
        "kafka_partition",
        "kafka_offset",
        "event.*"
    )
)

# 3. Écrire les événements dans MinIO Bronze
bronze_query = (
    events.writeStream
    .format("json")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("\nStreaming demarre avec succes !")
print("En attente des evenements Kafka...")
print("Pour arreter : Ctrl + C\n")

try:
    bronze_query.awaitTermination()
except KeyboardInterrupt:
    print("\nArret du streaming demande...")
finally:
    bronze_query.stop()
    spark.stop()
