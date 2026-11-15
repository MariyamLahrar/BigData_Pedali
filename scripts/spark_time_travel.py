
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Pedali-Time-Travel")
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

delta_path = "file:///opt/pedali/data/trips_delta"

print("\n===== TIME TRAVEL - VERSION 0 =====")

df = (
    spark.read.format("delta")
    .option("versionAsOf", 0)
    .load(delta_path)
)

print("Nombre de trajets :", df.count())

df.show(5, truncate=False)

spark.stop()
