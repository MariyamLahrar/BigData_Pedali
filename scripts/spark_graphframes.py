
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from graphframes import GraphFrame

# ==========================================
# PEDALI BIG DATA 2 - PARTIE 6
# Construction du graphe GraphFrames
# ==========================================

spark = (
    SparkSession.builder
    .appName("Pedali-Part6-GraphFrames")
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

VERTICES_PATH = "s3a://lake/silver/graph/vertices/"
EDGES_PATH = "s3a://lake/silver/graph/edges/"

print("\n========== PARTIE 6 : GRAPHFRAMES ==========")

# 1. Charger les sommets et les aretes
vertices = spark.read.parquet(VERTICES_PATH)
edges = spark.read.parquet(EDGES_PATH)

print("\n===== CHARGEMENT DES DONNEES =====")
print("Nombre de sommets :", vertices.count())
print("Nombre d'aretes :", edges.count())

# 2. Construire le graphe
graph = GraphFrame(vertices, edges)

print("\nGRAPHFRAME CREE AVEC SUCCES")

# 3. Verifier les sommets par ville
print("\n===== SOMMETS PAR VILLE =====")
graph.vertices.groupBy("city").count().orderBy("city").show()

# 4. Verifier les aretes par ville de depart
print("\n===== ARETES PAR VILLE =====")

edges_with_city = graph.edges.join(
    graph.vertices.select(
        col("id").alias("src"),
        col("city")
    ),
    on="src",
    how="left"
)

edges_with_city.groupBy("city").count().orderBy("city").show()

# 5. Verifier les aretes orphelines
ids = vertices.select("id")

invalid_src = edges.join(
    ids.withColumnRenamed("id", "src"),
    on="src",
    how="left_anti"
)

invalid_dst = edges.join(
    ids.withColumnRenamed("id", "dst"),
    on="dst",
    how="left_anti"
)

orphan_count = invalid_src.count() + invalid_dst.count()

print("\n===== VERIFICATION DES ARETES =====")
print("Aretes orphelines :", orphan_count)

if orphan_count == 0:
    print("VALIDATION REUSSIE : aucune arete orpheline.")
else:
    print("ATTENTION : aretes orphelines detectees.")

# 6. Calculer les degres
print("\n===== TOP 10 STATIONS CONNECTEES =====")

graph.degrees.orderBy(
    col("degree").desc()
).show(10, truncate=False)



# ==========================================
# ANALYSE DES TRAJETS RECIPROQUES
# ==========================================

print("\n===== TRAJETS RECIPROQUES =====")

reciproques = graph.find(
    "(a)-[ab]->(b); (b)-[ba]->(a)"
)

# Eviter de compter deux fois la meme paire
# et comparer uniquement les memes periodes.
reciproques = reciproques.filter(
    (col("a.id") < col("b.id")) &
    (col("ab.period") == col("ba.period"))
)

resultats = reciproques.select(
    col("a.id").alias("station_A"),
    col("b.id").alias("station_B"),
    col("ab.period").alias("periode"),
    col("ab.trip_count").alias("trajets_A_vers_B"),
    col("ba.trip_count").alias("trajets_B_vers_A")
)

print("Nombre de paires reciproques :", resultats.count())

resultats.show(10, truncate=False)

# ==========================================
# DETECTION DES CYCLES DE 3 STATIONS
# ==========================================

print("\n===== CYCLES DE 3 STATIONS =====")

cycles = graph.find(
    "(a)-[ab]->(b); (b)-[bc]->(c); (c)-[ca]->(a)"
)

# Stations distinctes, meme periode,
# et suppression des rotations du meme cycle.
cycles = cycles.filter(
    (col("a.id") != col("b.id")) &
    (col("b.id") != col("c.id")) &
    (col("c.id") != col("a.id")) &
    (col("a.id") < col("b.id")) &
    (col("a.id") < col("c.id")) &
    (col("ab.period") == col("bc.period")) &
    (col("bc.period") == col("ca.period"))
)

resultats_cycles = cycles.select(
    col("a.id").alias("station_A"),
    col("b.id").alias("station_B"),
    col("c.id").alias("station_C"),
    col("ab.period").alias("periode")
)

print("Nombre de cycles :", resultats_cycles.count())
resultats_cycles.show(10, truncate=False)

# ==========================================
# DEGRES PONDERES ET SOLDE DES STATIONS
# ==========================================

from pyspark.sql.functions import sum as spark_sum, coalesce, lit

print("\n===== DEGRES PONDERES =====")

entrees = edges.groupBy("dst", "period").agg(
    spark_sum("trip_count").alias("entrees")
).withColumnRenamed("dst", "id")

sorties = edges.groupBy("src", "period").agg(
    spark_sum("trip_count").alias("sorties")
).withColumnRenamed("src", "id")

flux = entrees.join(
    sorties,
    on=["id", "period"],
    how="full_outer"
).fillna(0, subset=["entrees", "sorties"])

flux = flux.withColumn(
    "solde",
    col("entrees") - col("sorties")
)

flux = flux.join(
    vertices.select("id", "city", "capacity"),
    on="id",
    how="left"
)

print("\n===== STATIONS QUI SE VIDENT =====")
flux.orderBy(col("solde").asc()).show(10, truncate=False)

print("\n===== STATIONS QUI SE REMPLISSENT =====")
flux.orderBy(col("solde").desc()).show(10, truncate=False)

print("\n===== TOTAL DES FLUX =====")
flux.agg(
    spark_sum("entrees").alias("total_entrees"),
    spark_sum("sorties").alias("total_sorties")
).show()



# ==========================================
# RECOMMANDATIONS DE REEQUILIBRAGE
# ==========================================

from pyspark.sql.functions import abs as spark_abs, round as spark_round, when

print("\n===== ANALYSE DU REEQUILIBRAGE =====")

# Calculer le desequilibre relatif a la capacite
reequilibrage = flux.withColumn(
    "taux_desequilibre",
    spark_round(
        spark_abs(col("solde")) / col("capacity") * 100,
        2
    )
)

# Seuil choisi pour notre analyse : 10 %
SEUIL = 10.0

reequilibrage = reequilibrage.withColumn(
    "recommandation",
    when(
        (col("solde") < 0) &
        (col("taux_desequilibre") >= SEUIL),
        "APPROVISIONNER"
    ).when(
        (col("solde") > 0) &
        (col("taux_desequilibre") >= SEUIL),
        "RETIRER DES VELOS"
    ).otherwise("SURVEILLER")
)

print("\n===== STATIONS PRIORITAIRES =====")

reequilibrage.filter(
    col("recommandation") != "SURVEILLER"
).select(
    "id",
    "city",
    "period",
    "capacity",
    "solde",
    "taux_desequilibre",
    "recommandation"
).orderBy(
    col("taux_desequilibre").desc()
).show(20, truncate=False)

print("\n===== RESUME DES RECOMMANDATIONS =====")

reequilibrage.groupBy(
    "city", "recommandation"
).count().orderBy(
    "city", "recommandation"
).show(truncate=False)


# ==========================================
# COMPARAISON GRAPHFRAMES / GRAPHX
# ==========================================

print("\n===== COMPARAISON DES DEGRES =====")

degres_comparaison = (
    graph.degrees
    .join(
        vertices.select("id", "city"),
        on="id",
        how="left"
    )
    .orderBy(col("degree").desc(), col("id"))
)

degres_comparaison.show(20, truncate=False)

print("Degre maximal GraphFrames :",
      degres_comparaison.agg(
          {"degree": "max"}
      ).first()[0])



# ==========================================
# EXPORT GRAPHFRAMES VERS MINIO GOLD
# ==========================================

print("\n===== EXPORT GOLD GRAPH ANALYSIS =====")

gold_degrees = (
    graph.degrees
    .join(
        vertices.select("id", "city"),
        on="id",
        how="left"
    )
    .withColumn("result_type", lit("degrees"))
)

gold_degrees.write \
    .mode("overwrite") \
    .partitionBy("city", "result_type") \
    .parquet("s3a://lake/gold/graph_analysis/graphframes_degrees/")

print("Export des degres termine avec succes")



# ==========================================
# EXPORT DES RECOMMANDATIONS VERS GOLD
# ==========================================

print("\n===== EXPORT REEQUILIBRAGE GOLD =====")

gold_reequilibrage = reequilibrage.withColumn(
    "result_type", lit("reequilibrage")
)

gold_reequilibrage.write \
    .mode("overwrite") \
    .partitionBy("city", "result_type") \
    .parquet(
        "s3a://lake/gold/graph_analysis/reequilibrage/"
    )

print("EXPORT REEQUILIBRAGE TERMINE AVEC SUCCES")

print("\nGRAPHFRAMES TERMINE AVEC SUCCES")

spark.stop()
