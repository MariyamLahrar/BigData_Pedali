
from pyspark.sql import SparkSession

# Créer la session Spark
spark = (
    SparkSession.builder
    .appName("Pedali-Spark-MinIO")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# Lire les données directement depuis MinIO
chemin = "s3a://pedali-data/trips_history.jsonl"

df = spark.read.json(chemin)

# Afficher les résultats
print("Nombre total de trajets depuis MinIO :", df.count())
df.show(5, truncate=False)

spark.stop()
