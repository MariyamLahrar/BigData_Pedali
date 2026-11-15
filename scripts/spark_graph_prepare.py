
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, when, count, avg,
    round as spark_round, lit, to_timestamp
)

# ==========================================
# PEDALI BIG DATA 2 - PARTIE 6
# Preparation des sommets et aretes
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Part6-Preparation-Graphe")
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

spark.sparkContext.setLogLevel("ERROR")

STATIONS = "s3a://lake/bronze/stations/stations.json"
TRIPS = "s3a://lake/silver/trips_clean/"

BASE = "s3a://lake/silver/graph/"
VERTICES = BASE + "vertices/"
EDGES = BASE + "edges/"
LOOPS = BASE + "loops/"
ORPHANS = BASE + "orphan_trips/"

print("\n========== PARTIE 6 : PREPARATION GRAPHE ==========")

# 1. Sommets : toutes les stations du referentiel
stations = (
    spark.read
    .option("multiLine", "true")
    .json(STATIONS)
)

vertices = (
    stations.select(
        col("station_id").alias("id"),
        col("name"),
        col("city"),
        col("district"),
        col("zone_type"),
        col("capacity").cast("int"),
        col("latitude").cast("double"),
        col("longitude").cast("double")
    )
    .filter(col("id").isNotNull())
    .dropDuplicates(["id"])
)

print("\nNombre de sommets :", vertices.count())

# 2. Trajets termines de la couche Silver
trips = spark.read.parquet(TRIPS)

trips = trips.filter(col("status") == "ENDED")

# Utiliser event_ts si present, sinon convertir event_time
if "event_ts" not in trips.columns:
    trips = trips.withColumn(
        "event_ts", to_timestamp(col("event_time"))
    )

# 3. Periode de deplacement
trips = trips.withColumn(
    "period",
    when(
        (hour("event_ts") >= 5) &
        (hour("event_ts") < 10), "matin"
    )
    .when(
        (hour("event_ts") >= 10) &
        (hour("event_ts") < 17), "journee"
    )
    .otherwise("soir")
)

# 4. Preparation des trajets orientes
raw_edges = trips.select(
    col("trip_id"),
    col("start_station").alias("src"),
    col("end_station").alias("dst"),
    col("period"),
    col("duration_s"),
    col("distance_km")
)

# 5. Verification des stations existantes
ids = vertices.select(col("id"))

valid_src = raw_edges.join(
    ids.withColumnRenamed("id", "src"),
    on="src",
    how="inner"
)

valid_both = valid_src.join(
    ids.withColumnRenamed("id", "dst"),
    on="dst",
    how="inner"
)

# Trajets orphelins conserves pour audit
orphans = raw_edges.join(
    valid_both.select("trip_id").distinct(),
    on="trip_id",
    how="left_anti"
)

print("Trajets orphelins :", orphans.count())

# 6. Boucles depart = arrivee, traitees a part
loops = valid_both.filter(col("src") == col("dst"))

print("Boucles :", loops.count())

# 7. Aretes sans boucles
valid_edges = valid_both.filter(
    col("src") != col("dst")
)

# 8. Agregation par couple et periode
edges = (
    valid_edges
    .groupBy("src", "dst", "period")
    .agg(
        count("*").alias("trip_count"),
        spark_round(
            avg("duration_s"), 2
        ).alias("avg_duration_s"),
        spark_round(
            avg("distance_km"), 3
        ).alias("avg_distance_km")
    )
    .withColumn("edge_type", lit("TRIP"))
)

print("\nNombre d'aretes agregees :", edges.count())

print("\n===== SOMMETS =====")
vertices.show(10, truncate=False)

print("\n===== ARETES =====")
edges.show(10, truncate=False)

print("\n===== SOMMETS PAR VILLE =====")
vertices.groupBy("city").count().orderBy("city").show()

# 9. Sauvegarde Parquet
vertices.write.mode("overwrite").parquet(VERTICES)
edges.write.mode("overwrite").parquet(EDGES)
loops.write.mode("overwrite").parquet(LOOPS)
orphans.write.mode("overwrite").parquet(ORPHANS)

print("\nPREPARATION GRAPHE TERMINEE AVEC SUCCES")
print("Vertices :", VERTICES)
print("Edges :", EDGES)
print("Loops :", LOOPS)
print("Orphans :", ORPHANS)

spark.stop()
