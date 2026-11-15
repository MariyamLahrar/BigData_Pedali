
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    min as spark_min,
    max as spark_max
)

# ==========================================
# PEDALI.MA - SPARK SQL ET DATAFRAMES
# ==========================================

# 1. Connexion au cluster Spark
spark = (
    SparkSession.builder
    .appName("Pedali-Spark-SQL")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n===== PEDALI.MA - SPARK SQL =====")
print("Version Spark :", spark.version)
print("Spark Master :", spark.sparkContext.master)

# ==========================================
# 2. CHARGEMENT DES DONNEES
# ==========================================

print("\n===== CHARGEMENT DU DATAFRAME =====")

df = spark.read.json(
    "file:///opt/pedali/data/trips_history.jsonl"
)

print("Schema du DataFrame :")
df.printSchema()

print("Nombre de trajets :", df.count())

print("\n===== APERCU DES DONNEES =====")
df.show(5, truncate=False)

# ==========================================
# 3. ANALYSE AVEC DATAFRAMES
# ==========================================

print("\n===== STATISTIQUES DES TRAJETS =====")

df.select(
    count("*").alias("nombre_trajets"),
    spark_sum("duree_minutes").alias("duree_totale"),
    avg("duree_minutes").alias("duree_moyenne"),
    spark_min("duree_minutes").alias("duree_min"),
    spark_max("duree_minutes").alias("duree_max")
).show()

# ==========================================
# 4. CLASSEMENT DES STATIONS DE DEPART
# ==========================================

print("\n===== STATIONS DE DEPART =====")

df.groupBy("station_depart") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# ==========================================
# 5. CLASSEMENT DES STATIONS D'ARRIVEE
# ==========================================

print("\n===== STATIONS D'ARRIVEE =====")

df.groupBy("station_arrivee") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

# ==========================================
# 6. REQUETES SPARK SQL
# ==========================================

df.createOrReplaceTempView("trajets")

print("\n===== SQL : DUREE MOYENNE PAR STATION =====")

spark.sql("""
    SELECT
        station_depart,
        COUNT(*) AS nombre_trajets,
        ROUND(AVG(duree_minutes), 2) AS duree_moyenne
    FROM trajets
    GROUP BY station_depart
    ORDER BY nombre_trajets DESC
""").show()

print("\n===== SQL : TRAJETS LONGS =====")

spark.sql("""
    SELECT
        station_depart,
        station_arrivee,
        duree_minutes
    FROM trajets
    WHERE duree_minutes >= 45
    ORDER BY duree_minutes DESC
    LIMIT 10
""").show()

# ==========================================
# 7. EXPORT AU FORMAT PARQUET
# ==========================================

print("\n===== EXPORT PARQUET =====")

df.write.mode("overwrite").parquet(
    "file:///opt/pedali/data/trips_parquet"
)

print("Export Parquet termine avec succes !")

# ==========================================
# 8. FIN DU PROGRAMME
# ==========================================

print("\n===== ANALYSE SPARK SQL TERMINEE =====")

spark.stop()
