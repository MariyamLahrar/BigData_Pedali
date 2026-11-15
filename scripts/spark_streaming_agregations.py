
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# ==========================================================
# PEDALI BIG DATA 2 - PARTIE 4
# Agrégations Kafka en temps réel par ville et statut
# ==========================================================

KAFKA_SERVERS = "kafka:19092"
TOPIC = "trips"

CHECKPOINT = "s3a://lake/checkpoints/part4_agregations/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part4-Aggregations")
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

print("\n========== PEDALI - AGREGATIONS ==========")
print("Topic Kafka :", TOPIC)

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
    .filter(col("city").isNotNull() & col("status").isNotNull())
)

counts = (
    events.groupBy("city", "status")
    .count()
    .orderBy("city", "status")
)

query = (
    counts.writeStream
    .format("console")
    .outputMode("complete")
    .option("truncate", "false")
    .option("numRows", 100)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)

print("\nStreaming des agregations demarre !")
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
