
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count,
    when, udf, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# ==========================================
# PEDALI - PARTIE 3 : SPARK SQL
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Partie3-SparkSQL")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ==========================================
# 1. SCHEMAS EXPLICITES
# ==========================================

schema_trips = StructType([
    StructField("trip_id", StringType()),
    StructField("user_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("vehicle_type", StringType()),
    StructField("start_station", StringType()),
    StructField("end_station", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("duration_s", IntegerType()),
    StructField("distance_km", DoubleType()),
    StructField("battery_pct", IntegerType()),
    StructField("price_mad", DoubleType()),
    StructField("event_time", StringType())
])

schema_stations = StructType([
    StructField("station_id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("district", StringType()),
    StructField("zone_type", StringType()),
    StructField("capacity", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])

# ==========================================
# 2. CHARGEMENT DEPUIS MINIO
# ==========================================

trips = spark.read.schema(schema_trips).json(
    "s3a://lake/bronze/history/trips_history_v2.jsonl"
)

stations = spark.read.schema(schema_stations).option(
    "multiLine", "true"
).json(
    "s3a://lake/bronze/stations/stations.json"
)

print("\n===== DONNEES =====")
print("Evenements :", trips.count())
print("Stations :", stations.count())

ended = trips.filter(col("status") == "ENDED")

# ==========================================
# 3. CHIFFRE D'AFFAIRES - DATAFRAME
# ==========================================

print("\n===== CA PAR VILLE - DATAFRAME =====")

ended.groupBy("city").agg(
    spark_round(
        spark_sum("price_mad"), 2
    ).alias("revenue_mad")
).orderBy("city").show()

# ==========================================
# 4. CHIFFRE D'AFFAIRES - SQL
# ==========================================

ended.createOrReplaceTempView("trips_ended")

print("\n===== CA PAR VILLE - SQL =====")

spark.sql("""
    SELECT
        city,
        ROUND(SUM(price_mad), 2) AS revenue_mad
    FROM trips_ended
    GROUP BY city
    ORDER BY city
""").show()

# ==========================================
# 5. JOINTURE AVEC LES STATIONS
# ==========================================

joined = ended.join(
    stations,
    ended.start_station == stations.station_id,
    "inner"
).select(
    ended["trip_id"],
    ended["city"],
    ended["duration_s"],
    ended["price_mad"],
    stations["district"],
    stations["zone_type"]
)

print("\n===== REVENUS PAR ZONE =====")

joined.groupBy("city", "zone_type").agg(
    count("*").alias("nb_trips"),
    spark_round(
        spark_sum("price_mad"), 2
    ).alias("revenue_mad")
).orderBy("city", "zone_type").show(50)

# ==========================================
# 6. UDF : CATEGORIE DE TRAJET
# ==========================================

def calculer_bande(duree):
    if duree is None:
        return None
    if duree < 600:
        return "short"
    if duree <= 1800:
        return "medium"
    return "long"

bande_udf = udf(calculer_bande, StringType())

avec_udf = ended.withColumn(
    "trip_band",
    bande_udf(col("duration_s"))
)

print("\n===== CLASSIFICATION UDF =====")
avec_udf.groupBy("trip_band").count().show()

# ==========================================
# 7. VERSION OPTIMISEE WHEN / OTHERWISE
# ==========================================

avec_when = ended.withColumn(
    "trip_band",
    when(col("duration_s") < 600, "short")
    .when(col("duration_s") <= 1800, "medium")
    .otherwise("long")
)

print("\n===== CLASSIFICATION WHEN =====")
avec_when.groupBy("trip_band").count().show()

# ==========================================
# 8. PLAN D'EXECUTION
# ==========================================

print("\n===== EXPLAIN TRUE - JOIN =====")
joined.explain(True)

# ==========================================
# 9. EXPORT PARQUET
# ==========================================

sortie = "s3a://lake/silver/trips_history/"

avec_when.write.mode("overwrite").partitionBy(
    "city"
).parquet(sortie)

print("\nExport Parquet termine :", sortie)



spark.stop()
print("\nPARTIE 3 TERMINEE")
