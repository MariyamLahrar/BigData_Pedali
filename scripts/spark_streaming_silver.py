
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, trim, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    LongType, DoubleType
)

# ==========================================
# PEDALI BIG DATA 2 - PARTIE 5
# STREAMING SILVER AVANCE
# ==========================================

KAFKA = "kafka:19092"
TOPIC = "trips"

STATIONS = "s3a://lake/bronze/stations/stations.json"

OUTPUT = "s3a://lake/silver/trips_streaming/"
CHECKPOINT = "s3a://lake/checkpoints/part5_silver_v1/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Streaming-Silver")
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
    StructField("trip_id", StringType()),
    StructField("user_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("start_station", StringType()),
    StructField("end_station", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("duration_s", LongType()),
    StructField("distance_km", DoubleType()),
    StructField("battery_pct", DoubleType()),
    StructField("price_mad", DoubleType()),
    StructField("event_time", StringType())
])

print("========== PARTIE 5 : STREAMING SILVER ==========")

# 1. Lecture Kafka
kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# 2. Extraction JSON
events = (
    kafka.select(
        col("value").cast("string").alias("raw_json"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    .withColumn("event", from_json(col("raw_json"), schema))
    .select("event.*", "kafka_partition",
            "kafka_offset", "kafka_timestamp")
)

# 3. Nettoyage
clean = (
    events
    .withColumn("trip_id", trim(col("trip_id")))
    .withColumn("city", trim(col("city")))
    .withColumn("status", trim(col("status")))
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .filter(
        col("trip_id").isNotNull()
        & (col("trip_id") != "")
        & col("city").isNotNull()
        & (col("city") != "")
        & col("event_ts").isNotNull()
        & col("vehicle_type").isin("BIKE", "EBIKE", "SCOOTER")
        & col("status").isin(
            "ENDED", "CANCELLED", "FAULT_REPORTED",
            "RESERVED", "UNLOCKED"
        )
        & (col("duration_s").isNull() | (col("duration_s") >= 0))
        & (col("distance_km").isNull() | (col("distance_km") >= 0))
        & (col("price_mad").isNull() | (col("price_mad") >= 0))
        & (
            col("battery_pct").isNull()
            | col("battery_pct").between(0, 100)
        )
    )
)

# 4. Watermark + deduplication
# Ne pas dedupliquer uniquement sur trip_id :
# un trajet peut posseder plusieurs statuts.
dedup = (
    clean
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates([
        "trip_id", "status", "event_ts", "vehicle_id"
    ])
)

# 5. Chargement du referentiel des stations
stations = (
    spark.read
    .option("multiLine", "true")
    .json(STATIONS)
    .dropDuplicates(["station_id"])
)

depart = stations.select(
    col("station_id").alias("start_id_ref"),
    col("name").alias("start_station_name"),
    col("district").alias("start_district"),
    col("zone_type").alias("start_zone_type"),
    col("capacity").alias("start_capacity")
)

arrivee = stations.select(
    col("station_id").alias("end_id_ref"),
    col("name").alias("end_station_name"),
    col("district").alias("end_district"),
    col("zone_type").alias("end_zone_type"),
    col("capacity").alias("end_capacity")
)

# 6. Enrichissement
enriched = (
    dedup
    .join(
        broadcast(depart),
        col("start_station") == col("start_id_ref"),
        "left"
    )
    .drop("start_id_ref")
    .join(
        broadcast(arrivee),
        col("end_station") == col("end_id_ref"),
        "left"
    )
    .drop("end_id_ref")
)

# 7. Ecriture Parquet en Streaming
query = (
    enriched.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", OUTPUT)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start()
)

print("Streaming Silver demarre.")
print("Destination :", OUTPUT)
print("Checkpoint :", CHECKPOINT)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Arret demande.")
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
