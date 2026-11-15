
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# ==========================================
# PEDALI BIG DATA 2 - PARTIE 5
# Enrichissement Silver avec les stations
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Enrichissement-Stations")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

SILVER_INPUT = "s3a://lake/silver/trips_clean/"
STATIONS_INPUT = "s3a://lake/bronze/stations/stations.json"
SILVER_OUTPUT = "s3a://lake/silver/trips_enriched/"

print("\n========== PARTIE 5 : ENRICHISSEMENT ==========")

# 1. Lecture des trajets Silver
trips = spark.read.parquet(SILVER_INPUT)

print("Trajets Silver :", trips.count())

# 2. Lecture des stations
stations = (
    spark.read
    .option("multiLine", "true")
    .json(STATIONS_INPUT)
)

print("Stations disponibles :", stations.count())

# 3. Preparation des stations de depart
stations_depart = stations.select(
    col("station_id").alias("start_station_id"),
    col("name").alias("start_station_name"),
    col("district").alias("start_district"),
    col("zone_type").alias("start_zone_type"),
    col("capacity").alias("start_capacity"),
    col("latitude").alias("start_latitude"),
    col("longitude").alias("start_longitude")
)

# 4. Preparation des stations d'arrivee
stations_arrivee = stations.select(
    col("station_id").alias("end_station_id"),
    col("name").alias("end_station_name"),
    col("district").alias("end_district"),
    col("zone_type").alias("end_zone_type"),
    col("capacity").alias("end_capacity"),
    col("latitude").alias("end_latitude"),
    col("longitude").alias("end_longitude")
)

# 5. Jointure avec la station de depart
enriched = trips.join(
    broadcast(stations_depart),
    trips["start_station"] == stations_depart["start_station_id"],
    "left"
).drop("start_station_id")

# 6. Jointure avec la station d'arrivee
enriched = enriched.join(
    broadcast(stations_arrivee),
    enriched["end_station"] == stations_arrivee["end_station_id"],
    "left"
).drop("end_station_id")

# 7. Verification
print("\n========== RESULTATS ==========")

total_enriched = enriched.count()
print("Trajets enrichis :", total_enriched)

print(
    "Stations de depart non trouvees :",
    enriched.filter(col("start_station_name").isNull()).count()
)

print(
    "Stations d'arrivee non trouvees :",
    enriched.filter(col("end_station_name").isNull()).count()
)

enriched.select(
    "trip_id",
    "city",
    "start_station",
    "start_station_name",
    "start_district",
    "end_station",
    "end_station_name",
    "end_district"
).show(10, truncate=False)

# 8. Sauvegarde dans MinIO Silver
enriched.write.mode("overwrite").parquet(SILVER_OUTPUT)

print("\nEnrichissement termine avec succes.")
print("Destination :", SILVER_OUTPUT)

spark.stop()
