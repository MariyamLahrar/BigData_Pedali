
import json
import time
from pyspark.sql import SparkSession

# ==========================================
# PEDALI.MA - PARTIE 2 : SPARK CORE / RDD
# ==========================================

DEBUT = time.perf_counter()

spark = (
    SparkSession.builder
    .appName("Pedali-Partie2-RDD-Officiel")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("\n========== INFORMATIONS SPARK ==========")
print("Version Spark :", spark.version)
print("Master :", sc.master)
print("Application :", sc.appName)
print("Parallelisme :", sc.defaultParallelism)

# ==========================================
# 1. CHARGEMENT DES DONNEES DEPUIS MINIO
# ==========================================

chemin = "s3a://lake/bronze/history/trips_history_v2.jsonl"

lignes = sc.textFile(chemin)
evenements = lignes.map(json.loads).cache()

total = evenements.count()

print("\n========== DONNEES ==========")
print("Nombre total d'evenements :", total)

# ==========================================
# 2. FILTRAGE DES TRAJETS TERMINES
# ==========================================

trajets_termines = evenements.filter(
    lambda t: t.get("status") == "ENDED"
).cache()

print(
    "Nombre de trajets ENDED :",
    trajets_termines.count()
)

# ==========================================
# 3. CHIFFRE D'AFFAIRES PAR VILLE
# ==========================================

ca_ville = (
    trajets_termines
    .map(
        lambda t: (
            t["city"],
            float(t.get("price_mad") or 0)
        )
    )
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
    .collect()
)

print("\n========== CA PAR VILLE ==========")

for ville, ca in ca_ville:
    print(f"{ville} : {ca:.2f} MAD")

# ==========================================
# 4. DUREE MOYENNE PAR VILLE ET VEHICULE
# ==========================================

durees = (
    trajets_termines
    .filter(lambda t: t.get("duration_s") is not None)
    .map(
        lambda t: (
            (t["city"], t["vehicle_type"]),
            (float(t["duration_s"]), 1)
        )
    )
    .reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1]
        )
    )
    .mapValues(
        lambda v: v[0] / v[1]
    )
    .sortByKey()
    .collect()
)

print("\n========== DUREE MOYENNE ==========")

for (ville, vehicule), secondes in durees:
    print(
        f"{ville} | {vehicule} : "
        f"{secondes / 60:.2f} minutes"
    )

# ==========================================
# 5. TOP 5 DES STATIONS DE DEPART
# ==========================================

top_stations = (
    trajets_termines
    .map(lambda t: (t["start_station"], 1))
    .reduceByKey(lambda a, b: a + b)
    .takeOrdered(
        5,
        key=lambda x: (-x[1], x[0])
    )
)

print("\n========== TOP 5 STATIONS ==========")

for station, nombre in top_stations:
    print(f"{station} : {nombre} trajets")

# ==========================================
# 6. LINEAGE RDD
# ==========================================

print("\n========== RDD LINEAGE ==========")

lineage = trajets_termines.map(
    lambda t: (t["city"], float(t.get("price_mad") or 0))
).reduceByKey(lambda a, b: a + b)

print(lineage.toDebugString().decode("utf-8"))

# ==========================================
# 7. TEMPS D'EXECUTION
# ==========================================

print("\n========== PERFORMANCE ==========")
print(
    f"Temps total : {time.perf_counter() - DEBUT:.2f} secondes"
)

trajets_termines.unpersist()
evenements.unpersist()
print("\nSpark UI disponible pendant 120 secondes...")
time.sleep(120)
spark.stop()

print("\nPARTIE 2 - EXECUTION TERMINEE")
