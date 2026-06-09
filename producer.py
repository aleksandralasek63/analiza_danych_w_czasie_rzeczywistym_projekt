
from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

# funkcja wyliczająca przebyty dystans
def haversine(lat1, lon1, lat2, lon2):
    R = 6371

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1))
        * cos(radians(lat2))
        * sin(dlon / 2) ** 2
    )

    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

# Połączenie z brokerem Kafka
# Każdy wysyłany słownik Python zostanie automatycznie
# zamieniony na JSON.
producer = KafkaProducer(
    bootstrap_servers="broker:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Liczba symulowanych taksówek
NUM_TAXIS = 100

# Słownik przechowujący aktualny stan każdej taksówki
taxis = {}

# Inicjalizacja floty
for taxi_id in range(1, NUM_TAXIS + 1):

    taxis[taxi_id] = {

        # Identyfikator taksówki
        "taxi_id": taxi_id,

        # Losowa pozycja w okolicach centrum Warszawy
        "lat": 52.2297 + random.uniform(-0.05, 0.05),
        "lon": 21.0122 + random.uniform(-0.05, 0.05),

        # Początkowo każda taksówka jest wolna
        "status": "available",

        # Aktualna prędkość
        "speed": 0,

        # Łączny zarobek kierowcy
        "earnings": 0.0,

        # Liczba zakończonych kursów
        "completed_rides": 0,

        # Przebyty dystans
        "distance_km": 0.0
    }

print("Producer started...")

# Główna pętla symulacji
while True:

    # Aktualizacja każdej taksówki
    for taxi in taxis.values():

        # Jeśli taksówka jest wolna
        if taxi["status"] == "available":

            # 3% szans na rozpoczęcie nowego kursu
            if random.random() < 0.03:

                taxi["status"] = "occupied"

                # Losowy czas kursu
                taxi["trip_time_left"] = random.randint(10, 30)

                # Losowa wartość kursu
                taxi["trip_price"] = round(
                    random.uniform(15, 80),
                    2
                )

        # Jeśli taksówka aktualnie wykonuje kurs
        else:

            # Zmniejszamy pozostały czas kursu
            taxi["trip_time_left"] -= 1

            # Kurs zakończony
            if taxi["trip_time_left"] <= 0:

                taxi["status"] = "available"

                # Dodajemy zarobek z kursu
                taxi["earnings"] += taxi["trip_price"]

                # Zwiększamy licznik wykonanych kursów
                taxi["completed_rides"] += 1

        # Symulacja ruchu po mieście
        # Taksówka przesuwa się o niewielką losową wartość
        old_lat = taxi["lat"]
        old_lon = taxi["lon"]
        
        new_lat = old_lat + random.uniform(-0.001, 0.001)
        new_lon = old_lon + random.uniform(-0.001, 0.001)
        
        trip_distance = haversine(
            old_lat,
            old_lon,
            new_lat,
            new_lon
        )
        
        taxi["distance_km"] += trip_distance
        
        taxi["lat"] = new_lat
        taxi["lon"] = new_lon

        # Prędkość zależy od statusu
        if taxi["status"] == "occupied":
            taxi["speed"] = random.randint(20, 60)
        else:
            taxi["speed"] = random.randint(0, 15)

        # Dane wysyłane do Kafki
        event = {

            "taxi_id": taxi["taxi_id"],

            # Znacznik czasu zdarzenia
            "timestamp": datetime.now().isoformat(),

            # Aktualna pozycja GPS
            "lat": taxi["lat"],
            "lon": taxi["lon"],

            # Status taksówki
            "status": taxi["status"],

            # Aktualna prędkość
            "speed": taxi["speed"],

            # Łączny przychód kierowcy
            "earnings": round(taxi["earnings"], 2),

            # Przebyte kilometry
            "distance_km": round(
                taxi["distance_km"],
                2
            ),

            # Liczba wykonanych kursów
            "completed_rides": taxi["completed_rides"]
        }

        # Wysłanie wiadomości do topicu Kafka
        producer.send(
            "taxi-events",
            value=event
        )

    # Wymuszenie wysłania wszystkich wiadomości
    producer.flush()

    print(
        f"Sent {NUM_TAXIS} taxi events at "
        f"{datetime.now().strftime('%H:%M:%S')}"
    )

    # Aktualizacja co sekundę
    time.sleep(1)
