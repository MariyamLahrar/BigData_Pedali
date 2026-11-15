
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================================
# 1. CONFIGURATION SPARK ET MINIO
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Export-Graphe-Casablanca")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # ==========================================
    # 2. LECTURE DES DONNEES SILVER
    # ==========================================

    print("\n===== CHARGEMENT DES DONNEES =====")

    vertices = spark.read.parquet(
        "s3a://lake/silver/graph/vertices/"
    )

    edges = spark.read.parquet(
        "s3a://lake/silver/graph/edges/"
    )

    # ==========================================
    # 3. STATIONS DE CASABLANCA
    # ==========================================

    casablanca = vertices.filter(
        col("city") == "Casablanca"
    )

    # ==========================================
    # 4. TRAJETS DE CASABLANCA
    # ==========================================

    station_ids = casablanca.select("id")

    edges_casa = (
        edges
        .join(
            station_ids.select(col("id").alias("src")),
            on="src",
            how="inner"
        )
        .join(
            station_ids.select(col("id").alias("dst")),
            on="dst",
            how="inner"
        )
    )

    # ==========================================
    # 5. VERIFICATION
    # ==========================================

    nb_stations = casablanca.count()
    nb_aretes = edges_casa.count()

    print("\n===== GRAPHE CASABLANCA =====")
    print("Nombre de stations :", nb_stations)
    print("Nombre d'aretes :", nb_aretes)

    # ==========================================
    # 6. EXPORT CSV VERS MINIO
    # ==========================================

    print("\n===== EXPORT DES STATIONS =====")

    (
        casablanca.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(
            "s3a://lake/gold/graph_analysis/export_csv/vertices_casa/"
        )
    )

    print("Export stations termine")

    print("\n===== EXPORT DES ARETES =====")

    (
        edges_casa.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(
            "s3a://lake/gold/graph_analysis/export_csv/edges_casa/"
        )
    )

    print("Export aretes termine")

    # ==========================================
    # 7. FIN
    # ==========================================

    print("\n======================================")
    print("EXPORT CASABLANCA TERMINE AVEC SUCCES")
    print("Stations :", nb_stations)
    print("Aretes :", nb_aretes)
    print("Destination : MinIO / lake / gold")
    print("======================================")

finally:
    spark.stop()
