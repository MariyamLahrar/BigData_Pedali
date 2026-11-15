
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, session_window, count,
    sum as spark_sum, round as spark_round
)

# ==========================================================
# PEDALI BIG DATA 2 - PARTIE 5
# FENETRES TEMPORELLES : TUMBLING, SLIDING, SESSION
# ==========================================================

SILVER_PATH = "s3a://lake/silver/trips_clean/"

spark = (
    SparkSession.builder
    .appName("Pedali-Part5-Windows")
    .master("spark://spark-master:7077")
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

    df = (
        spark.read.parquet(SILVER_PATH)
        .filter(
            col("event_ts").isNotNull()
            & col("city").isNotNull()
        )
    )

    print("Nombre de lignes :", df.count())

    # ======================================================
    # 1. TUMBLING WINDOW : 10 MINUTES
    # ======================================================

    print("\n===== TUMBLING WINDOW 10 MIN =====")

    tumbling = (
        df.groupBy(
            window(col("event_ts"), "10 minutes"),
            col("city")
        )
        .agg(
            count("*").alias("total_evenements")
        )
        .select(
            col("window.start").alias("debut"),
            col("window.end").alias("fin"),
            col("city"),
            col("total_evenements")
        )
        .orderBy("debut", "city")
    )

    tumbling.show(30, truncate=False)

    # ======================================================
    # 2. SLIDING WINDOW : 10 MIN / PAS DE 5 MIN
    # ======================================================

    print("\n===== SLIDING WINDOW 10 MIN / 5 MIN =====")

    sliding = (
        df.groupBy(
            window(
                col("event_ts"),
                "10 minutes",
                "5 minutes"
            ),
            col("city")
        )
        .agg(
            count("*").alias("total_evenements")
        )
        .select(
            col("window.start").alias("debut"),
            col("window.end").alias("fin"),
            col("city"),
            col("total_evenements")
        )
        .orderBy("debut", "city")
    )

    sliding.show(30, truncate=False)

    # ======================================================
    # 3. SESSION WINDOW : 5 MIN PAR UTILISATEUR
    # ======================================================

    print("\n===== SESSION WINDOW 5 MIN =====")

    sessions = (
        df.filter(col("user_id").isNotNull())
        .groupBy(
            col("user_id"),
            session_window(col("event_ts"), "5 minutes")
        )
        .agg(
            count("*").alias("total_evenements"),
            spark_round(
                spark_sum("price_mad"), 2
            ).alias("montant_total_mad")
        )
        .select(
            col("user_id"),
            col("session_window.start").alias("debut"),
            col("session_window.end").alias("fin"),
            col("total_evenements"),
            col("montant_total_mad")
        )
        .orderBy("user_id", "debut")
    )

    sessions.show(30, truncate=False)

    print("\n===== FENETRES TEMPORELLES TERMINEES =====")

finally:
    spark.stop()
