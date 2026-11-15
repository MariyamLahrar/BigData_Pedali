
from pyspark.sql import SparkSession

# ==========================================
# PEDALI.MA - DELTA LAKE
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Delta-Lake")
    .master("spark://spark-master:7077")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# 1. Charger les trajets historiques
print("\n===== CHARGEMENT DES DONNEES =====")

df = spark.read.json(
    "file:///opt/pedali/data/trips_history.jsonl"
)

print("Nombre de trajets :", df.count())

# 2. Enregistrer au format Delta Lake
print("\n===== EXPORT DELTA LAKE =====")

delta_path = "file:///opt/pedali/data/trips_delta"

df.write.format("delta") \
    .mode("overwrite") \
    .save(delta_path)

print("Export Delta Lake termine !")

# 3. Relire les donnees Delta
print("\n===== VERIFICATION DELTA LAKE =====")

df_delta = spark.read.format("delta").load(delta_path)

print("Nombre de trajets Delta :", df_delta.count())

df_delta.show(5, truncate=False)

print("\n===== DELTA LAKE TERMINE =====")

spark.stop()
