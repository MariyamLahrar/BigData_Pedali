
import json
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

KAFKA_SERVER = "localhost:9092"
TOPIC = "trips"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

# On utilise des minutes rondes pour des fenetres de 5 minutes.
# Le watermark Spark est de 10 minutes.

base_time = datetime.now(timezone.utc).replace(
    second=0, microsecond=0
)

def envoyer_evenement(nom, event_time):
    evenement = {
        "trip_id": str(uuid.uuid4()),
        "city": "Rabat",
        "status": "ENDED",
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S")
    }

    producer.send(TOPIC, value=evenement).get(timeout=15)
    producer.flush()

    print(
        f"{nom} | "
        f"ville={evenement['city']} | "
        f"heure={evenement['event_time']}"
    )


print("\n========== TEST WATERMARK PEDALI ==========")

# 1. Evenement de reference : T
envoyer_evenement("TEST_1_REFERENCE", base_time)

# Laisser Spark traiter le premier evenement.
print("\nAttente de 45 secondes...")
time.sleep(45)

# 2. Evenement plus recent : T + 20 minutes.
# Il permet de faire avancer le watermark.
envoyer_evenement(
    "TEST_2_AVANCE_WATERMARK",
    base_time + timedelta(minutes=20)
)

# Laisser Spark terminer le micro-batch et avancer son watermark.
print("\nAttente de 60 secondes...")
time.sleep(60)

# 3. Evenement en retard : T + 5 minutes.
# Il est 15 minutes plus ancien que le maximum T + 20.
envoyer_evenement(
    "TEST_3_RETARD_15_MIN",
    base_time + timedelta(minutes=5)
)

print("\nTous les evenements de test ont ete envoyes.")
print("Observer les prochains Batch dans Spark.")

producer.close()
