
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Pedali-Inspection-Bronze")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

bronze = spark.read.json("s3a://lake/bronze/trips_raw/")

print("\n========== SCHEMA BRONZE ==========")
bronze.printSchema()

print("\n========== EXEMPLES ==========")
bronze.show(5, truncate=False)

print("\n========== NOMBRE DE LIGNES ==========")
print(bronze.count())

spark.stop()
