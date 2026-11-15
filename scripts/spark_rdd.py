
import json
from pyspark.sql import SparkSession

# ==========================================
# PEDALI.MA - ANALYSE BIG DATA AVEC SPARK RDD
# ==========================================

# 1. Connexion au cluster Spark
spark = (
    SparkSession.builder
    .appName("Pedali-RDD-Analysis")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# 2. Informations du cluster
print("\n===== PEDALI.MA - SPARK =====")
print("Version Spark :", spark.version)
print("Spark Master :", sc.master)
print("Application :", sc.appName)
print("Parallélisme :", sc.defaultParallelism)

# ==========================================
# 3. CHARGEMENT DES DONNEES JSONL
# ==========================================

print("\n===== CHARGEMENT DES TRAJETS =====")

fichier = "file:///tmp/trips_history.jsonl"

rdd = sc.textFile(fichier)
rdd = rdd.map(lambda ligne: json.loads(ligne))

# ==========================================
# 4. ANALYSE GENERALE DES TRAJETS
# ==========================================

print("\n===== ANALYSE DES TRAJETS =====")

nombre_trajets = rdd.count()
print("Nombre de trajets :", nombre_trajets)

durees = rdd.map(
    lambda trajet: trajet["duree_minutes"]
)

duree_totale = durees.sum()
print("Duree totale :", duree_totale, "minutes")

duree_moyenne = durees.mean()
print("Duree moyenne :", round(duree_moyenne, 2), "minutes")

duree_min = durees.min()
print("Duree minimale :", duree_min, "minutes")

duree_max = durees.max()
print("Duree maximale :", duree_max, "minutes")

# ==========================================
# 5. ANALYSE DES STATIONS DE DEPART
# ==========================================

print("\n===== STATIONS DE DEPART =====")

stations_depart = (
    rdd.map(
        lambda trajet: (trajet["station_depart"], 1)
    )
    .reduceByKey(lambda a, b: a + b)
)

classement_depart = stations_depart.sortBy(
    lambda station: station[1],
    ascending=False
)

for station, nombre in classement_depart.collect():
    print(station, ":", nombre, "departs")

# ==========================================
# 6. ANALYSE DES STATIONS D'ARRIVEE
# ==========================================

print("\n===== STATIONS D'ARRIVEE =====")

stations_arrivee = (
    rdd.map(
        lambda trajet: (trajet["station_arrivee"], 1)
    )
    .reduceByKey(lambda a, b: a + b)
)

classement_arrivee = stations_arrivee.sortBy(
    lambda station: station[1],
    ascending=False
)

for station, nombre in classement_arrivee.collect():
    print(station, ":", nombre, "arrivees")

# ==========================================
# 7. TRAJET LE PLUS LONG
# ==========================================

print("\n===== TRAJET LE PLUS LONG =====")

trajet_long = rdd.max(
    key=lambda trajet: trajet["duree_minutes"]
)

print("Depart :", trajet_long["station_depart"])
print("Arrivee :", trajet_long["station_arrivee"])
print("Duree :", trajet_long["duree_minutes"], "minutes")

# ==========================================
# 8. FIN DU PROGRAMME
# ==========================================

print("\n===== ANALYSE TERMINEE =====")

spark.stop()
