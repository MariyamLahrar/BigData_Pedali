
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, to_timestamp, row_number
)
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Silver-Nettoyage")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

BRONZE = "s3a://lake/bronze/trips_raw/"
SILVER = "s3a://lake/silver/trips_clean/"

print("\n========== PARTIE 5 : SILVER ==========")

# 1. Lecture des donnees Bronze
bronze = spark.read.json(BRONZE)

total_bronze = bronze.count()
print("Evenements Bronze :", total_bronze)

# 2. Nettoyage des champs texte et conversion de date
clean = (
    bronze
    .withColumn("trip_id", trim(col("trip_id")))
    .withColumn("city", trim(col("city")))
    .withColumn("status", trim(col("status")))
    .withColumn("vehicle_type", trim(col("vehicle_type")))
    .withColumn("event_ts", to_timestamp(col("event_time")))
)

# 3. Validation des champs obligatoires
clean = clean.filter(
    col("trip_id").isNotNull()
    & (col("trip_id") != "")
    & col("city").isNotNull()
    & (col("city") != "")
    & col("status").isNotNull()
    & col("event_ts").isNotNull()
    & col("vehicle_type").isin("BIKE", "EBIKE", "SCOOTER")
    & col("status").isin(
        "ENDED", "CANCELLED", "FAULT_REPORTED",
        "RESERVED", "UNLOCKED"
    )
)

# 4. Controle des valeurs numeriques
clean = clean.filter(
    (col("duration_s").isNull() | (col("duration_s") >= 0))
    & (col("distance_km").isNull() | (col("distance_km") >= 0))
    & (col("price_mad").isNull() | (col("price_mad") >= 0))
    & (
        col("battery_pct").isNull()
        | col("battery_pct").between(0, 100)
    )
)

total_valides = clean.count()
print("Evenements valides :", total_valides)

# 5. Deduplication des evenements identiques
# Un meme trajet peut avoir plusieurs statuts.
# On ne deduplique donc PAS uniquement sur trip_id.
cles_doublon = [
    "trip_id",
    "status",
    "event_time",
    "vehicle_id"
]

fenetre = Window.partitionBy(*cles_doublon).orderBy(
    col("kafka_timestamp").asc_nulls_last(),
    col("kafka_partition").asc_nulls_last(),
    col("kafka_offset").asc_nulls_last()
)

silver = (
    clean
    .withColumn("_rang", row_number().over(fenetre))
    .filter(col("_rang") == 1)
    .drop("_rang")
)

total_silver = silver.count()

print("Doublons retires :", total_valides - total_silver)
print("Evenements Silver :", total_silver)

# 6. Ecriture Parquet
silver.write.mode("overwrite").parquet(SILVER)

print("\n========== RESULTAT SILVER ==========")
silver.select(
    "trip_id", "city", "status",
    "vehicle_type", "event_ts", "price_mad"
).show(10, truncate=False)

print("Destination :", SILVER)
print("Nettoyage Silver termine avec succes.")

spark.stop()
