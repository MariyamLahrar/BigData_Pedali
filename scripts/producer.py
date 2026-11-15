
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer


# ==========================================
# PEDALI.MA - GENERATEUR BIG DATA
# ==========================================

VILLES = ["Casablanca", "Rabat", "Marrakech", "Tanger"]
TYPES_VEHICULES = ["BIKE", "EBIKE", "SCOOTER"]
STATUTS = [
    "RESERVED",
    "UNLOCKED",
    "ENDED",
    "CANCELLED",
    "FAULT_REPORTED"
]

ZONES = [
    "gare",
    "universite",
    "plage",
    "centre_affaires",
    "residentiel",
    "tourisme"
]

QUARTIERS = {
    "Casablanca": ["Maarif", "Anfa", "Sidi Maarouf", "Ain Diab"],
    "Rabat": ["Agdal", "Hassan", "Hay Riad", "Souissi"],
    "Marrakech": ["Gueliz", "Medina", "Hivernage", "Menara"],
    "Tanger": ["Malabata", "Centre", "Iberia", "Boukhalef"]
}

COORDONNEES = {
    "Casablanca": (33.5731, -7.5898),
    "Rabat": (34.0209, -6.8416),
    "Marrakech": (31.6295, -7.9811),
    "Tanger": (35.7595, -5.8340)
}

CODES = {
    "Casablanca": "CAS",
    "Rabat": "RAB",
    "Marrakech": "MAR",
    "Tanger": "TAN"
}

RNG = random.Random(42)


# ==========================================
# GENERATION DES 400 STATIONS
# ==========================================

def generer_stations():
    stations = []

    for ville in VILLES:
        latitude, longitude = COORDONNEES[ville]

        for i in range(1, 101):
            quartier = QUARTIERS[ville][(i - 1) % 4]

            station = {
                "station_id": f"S-{CODES[ville]}-{i:03d}",
                "name": f"Station {quartier} {i}",
                "city": ville,
                "district": quartier,
                "zone_type": ZONES[(i - 1) % len(ZONES)],
                "capacity": RNG.randint(10, 40),
                "latitude": round(latitude + RNG.uniform(-0.04, 0.04), 6),
                "longitude": round(longitude + RNG.uniform(-0.04, 0.04), 6)
            }

            stations.append(station)

    return stations


STATIONS = generer_stations()

STATIONS_PAR_VILLE = {
    ville: [s for s in STATIONS if s["city"] == ville]
    for ville in VILLES
}


# ==========================================
# GENERATION DES EVENEMENTS
# ==========================================

def generer_evenement(numero, temps_reel=False):
    ville = RNG.choice(VILLES)
    stations_ville = STATIONS_PAR_VILLE[ville]

    depart, arrivee = RNG.sample(stations_ville, 2)

    vehicle_type = RNG.choice(TYPES_VEHICULES)

    status = RNG.choices(
        STATUTS,
        weights=[8, 8, 72, 7, 5],
        k=1
    )[0]

    maintenant = datetime.now(timezone.utc)

    if temps_reel:
        event_time = maintenant
    else:
        event_time = maintenant - timedelta(
            minutes=RNG.randint(0, 10000)
        )

    # Environ 5 % d'evenements en retard
    if RNG.random() < 0.05:
        event_time -= timedelta(
            minutes=RNG.randint(11, 15)
        )

    termine = status == "ENDED"

    duration_s = RNG.randint(300, 3600) if termine else None
    distance_km = round(RNG.uniform(0.5, 12.0), 2) if termine else None

    if termine:
        prix_base = {
            "BIKE": 3.0,
            "EBIKE": 5.0,
            "SCOOTER": 6.0
        }[vehicle_type]

        price_mad = round(
            prix_base + (duration_s / 60) * 0.45,
            2
        )
    else:
        price_mad = None

    return {
        "trip_id": f"T-{numero:08d}",
        "user_id": f"U-{RNG.randint(1, 20000):05d}",
        "vehicle_id": f"V-{vehicle_type}-{RNG.randint(1, 5000):05d}",
        "vehicle_type": vehicle_type,
        "start_station": depart["station_id"],
        "end_station": arrivee["station_id"] if termine else None,
        "city": ville,
        "status": status,
        "duration_s": duration_s,
        "distance_km": distance_km,
        "battery_pct": (
            None if vehicle_type == "BIKE"
            else RNG.randint(10, 100)
        ),
        "price_mad": price_mad,
        "event_time": event_time.isoformat().replace("+00:00", "Z")
    }


# ==========================================
# ARGUMENTS
# ==========================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--mode",
    choices=["file", "kafka"],
    default="file"
)

parser.add_argument(
    "--n",
    type=int,
    default=50000
)

parser.add_argument(
    "--rate",
    type=float,
    default=20.0
)

parser.add_argument(
    "--interval",
    type=float,
    default=None
)

args = parser.parse_args()

if args.n < 1:
    parser.error("--n doit etre superieur a zero")

if args.rate <= 0:
    parser.error("--rate doit etre superieur a zero")

if args.interval is not None and args.interval < 0:
    parser.error("--interval ne peut pas etre negatif")


# ==========================================
# DOSSIERS ET REFERENTIEL STATIONS
# ==========================================

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)

stations_path = DATA / "stations.json"

with stations_path.open("w", encoding="utf-8") as fichier:
    json.dump(STATIONS, fichier, ensure_ascii=False, indent=2)

print("Referentiel stations :", stations_path)
print("Nombre de stations :", len(STATIONS))


# ==========================================
# MODE FILE : HISTORIQUE
# ==========================================

if args.mode == "file":

    chemin = DATA / "trips_history_v2.jsonl"

    doublons = 0
    precedent = None

    with chemin.open("w", encoding="utf-8") as fichier:
        for i in range(1, args.n + 1):

            if precedent is not None and RNG.random() < 0.02:
                evenement = precedent.copy()
                doublons += 1
            else:
                evenement = generer_evenement(i)
                precedent = evenement.copy()

            fichier.write(
                json.dumps(evenement, ensure_ascii=False) + "\n"
            )

    print("\n===== PEDALI - MODE FILE =====")
    print("Fichier :", chemin)
    print("Nombre d'evenements :", args.n)
    print("Doublons simules :", doublons)
    print("Generation terminee !")


# ==========================================
# MODE KAFKA : STREAMING
# ==========================================

elif args.mode == "kafka":

    intervalle = (
        args.interval
        if args.interval is not None
        else 1.0 / args.rate
    )

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda valeur: json.dumps(
            valeur, ensure_ascii=False
        ).encode("utf-8")
    )

    print("\n===== PEDALI - MODE KAFKA =====")
    print("Topic : trips")
    print("Debit cible :", args.rate, "evenements/s")
    print("Nombre d'evenements :", args.n)

    precedent = None

    try:
        for i in range(1, args.n + 1):

            if precedent is not None and RNG.random() < 0.02:
                evenement = precedent.copy()
            else:
                evenement = generer_evenement(
                    i, temps_reel=True
                )
                precedent = evenement.copy()

            producer.send("trips", value=evenement)

            if i % 100 == 0:
                print("Evenements envoyes :", i)

            time.sleep(intervalle)

        producer.flush()
        print("Envoi termine !")

    finally:
        producer.close()
