
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    count,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType
)

# ==========================================
# PEDALI.MA - KAFKA STRUCTURED STREAMING
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Kafka-Streaming")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("===== PEDALI.MA - STREAMING =====")

# 1. Schema des messages JSON
schema = StructType([
    StructField("trip_id", IntegerType()),
    StructField("station_depart", StringType()),
    StructField("station_arrivee", StringType()),
    StructField("date_depart", StringType()),
    StructField("duree_minutes", IntegerType())
])

# 2. Lecture du topic Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "trips")
    .option("startingOffsets", "latest")
    .load()
)

# 3. Conversion des messages JSON
trajets_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS message")
    .select(from_json(col("message"), schema).alias("trajet"))
    .select("trajet.*")
)

# 4. Analyse des stations en temps reel
statistiques = (
    trajets_df
    .groupBy("station_depart")
    .agg(
        count("*").alias("nombre_trajets"),
        avg("duree_minutes").alias("duree_moyenne")
    )
)

# 5. Affichage en continu
query = (
    statistiques.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation",
            "/opt/pedali/data/checkpoints/streaming")
    .trigger(processingTime="5 seconds")
    .start()
)

print("Streaming demarre !")
print("En attente de nouveaux trajets Kafka...")

query.awaitTermination()
