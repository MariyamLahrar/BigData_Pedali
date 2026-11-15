
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, count, avg, sum as spark_sum,
    round as spark_round
)

# ==========================================================
# PEDALI BIG DATA 2 - PARTIE 5
# GOLD DELTA - KPI PAR VILLE / FENETRE DE 10 MINUTES
# ==========================================================

SILVER_PATH = "s3a://lake/silver/trips_clean/"
GOLD_PATH = "s3a://lake/gold/kpi_city_10min/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Delta-Gold")
    .master("spark://spark-master:7077")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("\n===== LECTURE SILVER V2 =====")

    silver = spark.read.parquet(SILVER_PATH)

    silver = silver.filter(
        col("event_ts").isNotNull()
        & col("city").isNotNull()
    )

    print("Nombre de lignes Silver :", silver.count())

    # Conserver les trajets termines pour les KPI
    completed = silver.filter(col("status") == "ENDED")

    print("Trajets termines :", completed.count())

    # Fenetre fixe de 10 minutes (tumbling)
    print("\n===== CALCUL KPI 10 MINUTES =====")

    gold = (
        completed
        .groupBy(
            window(col("event_ts"), "10 minutes"),
            col("city")
        )
        .agg(
            count("*").alias("nombre_trajets"),
            spark_round(
                avg("duration_s"), 2
            ).alias("duree_moyenne_s"),
            spark_round(
                spark_sum("price_mad"), 2
            ).alias("chiffre_affaires_mad")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("nombre_trajets"),
            col("duree_moyenne_s"),
            col("chiffre_affaires_mad")
        )
        .orderBy("window_start", "city")
    )

    gold.show(30, truncate=False)

    # Securite : ne pas ecraser un Gold deja existant
    print("\n===== ENREGISTREMENT DELTA GOLD =====")

    (
        gold.write
        .format("delta")
        .mode("errorifexists")
        .save(GOLD_PATH)
    )

    print("Delta Gold enregistre :", GOLD_PATH)

    # Relire le resultat
    print("\n===== VERIFICATION DELTA GOLD =====")

    gold_check = spark.read.format("delta").load(GOLD_PATH)

    print("Nombre de KPI :", gold_check.count())
    gold_check.show(20, truncate=False)

    # Historique Delta
    print("\n===== DESCRIBE HISTORY =====")

    spark.sql(
        "DESCRIBE HISTORY delta.`" + GOLD_PATH + "`"
    ).select(
        "version", "timestamp", "operation"
    ).show(truncate=False)

    print("\nPARTIE 5 - DELTA GOLD TERMINE !")

finally:
    spark.stop()
