
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Pedali-Inspection-Stations")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n========== STATIONS PEDALI ==========")

stations = spark.read.option(
    "multiLine", "true"
).json("s3a://lake/bronze/stations/stations.json")

print("\n========== SCHEMA ==========")
stations.printSchema()

print("\n========== 5 EXEMPLES ==========")
stations.show(5, truncate=False)

print("\n========== NOMBRE DE STATIONS ==========")
print(stations.count())

spark.stop()
