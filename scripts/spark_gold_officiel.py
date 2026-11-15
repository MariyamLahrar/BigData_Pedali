
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum,
    avg, round as spark_round, when, lit
)

# Configuration Spark et MinIO
spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Gold-KPI")
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

SOURCE = "s3a://lake/silver/trips_streaming/"
GOLD_VILLES = "s3a://lake/gold/kpis_villes/"
GOLD_VEHICULES = "s3a://lake/gold/kpis_vehicules/"

print("\n========== PARTIE 5 : COUCHE GOLD ==========")

# Lecture des données Silver
trips = spark.read.parquet(SOURCE)
print("Evenements Silver :", trips.count())

# Seuls les trajets terminés comptent pour les KPI métier
ended = trips.filter(col("status") == "ENDED")

# KPI par ville
kpi_villes = (
    ended.groupBy("city")
    .agg(
        count("*").alias("nombre_trajets"),
        spark_round(
            spark_sum("price_mad"), 2
        ).alias("chiffre_affaires_mad"),
        spark_round(
            spark_sum("distance_km"), 2
        ).alias("distance_totale_km"),
        spark_round(
            avg("duration_s") / 60, 2
        ).alias("duree_moyenne_minutes")
    )
    .orderBy("city")
)

print("\n===== KPI PAR VILLE =====")
kpi_villes.show(truncate=False)

kpi_villes.write.mode("overwrite").parquet(GOLD_VILLES)

# KPI par type de véhicule
kpi_vehicules = (
    ended.groupBy("vehicle_type")
    .agg(
        count("*").alias("nombre_trajets"),
        spark_round(
            spark_sum("price_mad"), 2
        ).alias("chiffre_affaires_mad"),
        spark_round(
            avg("distance_km"), 2
        ).alias("distance_moyenne_km")
    )
    .orderBy("vehicle_type")
)

print("\n===== KPI PAR TYPE DE VEHICULE =====")
kpi_vehicules.show(truncate=False)

kpi_vehicules.write.mode("overwrite").parquet(GOLD_VEHICULES)

print("\nCOUCHE GOLD CREEE AVEC SUCCES")
print("Villes :", GOLD_VILLES)
print("Vehicules :", GOLD_VEHICULES)

spark.stop()
