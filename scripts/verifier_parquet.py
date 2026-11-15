
from pyspark.sql import SparkSession

# Creer une session Spark locale
spark = (
    SparkSession.builder
    .appName("Pedali-Verification-Parquet")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Lire les fichiers Parquet
df = spark.read.parquet(
    "file:///opt/pedali/data/trips_parquet"
)

# Verifier les donnees
print("\n===== VERIFICATION PARQUET =====")
print("Nombre de trajets Parquet :", df.count())

print("\n===== APERCU DES TRAJETS =====")
df.show(5, truncate=False)

print("\n===== VERIFICATION TERMINEE =====")

spark.stop()
