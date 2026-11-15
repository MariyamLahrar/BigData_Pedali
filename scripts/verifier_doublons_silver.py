
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("Pedali-Verification-Doublons-Silver")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

SOURCE = "s3a://lake/silver/trips_streaming/"

df = spark.read.parquet(SOURCE)

total = df.count()

doublons = (
    df.groupBy("trip_id", "status", "event_ts", "vehicle_id")
    .agg(count("*").alias("occurrences"))
    .filter(col("occurrences") > 1)
)

nombre_groupes_doublons = doublons.count()

print("\n===== VERIFICATION DES DOUBLONS SILVER =====")
print("Nombre total d'evenements :", total)
print("Groupes de doublons :", nombre_groupes_doublons)

if nombre_groupes_doublons == 0:
    print("VALIDATION REUSSIE : aucun doublon detecte.")
else:
    print("ATTENTION : doublons detectes.")
    doublons.show(20, truncate=False)

spark.stop()
