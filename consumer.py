
from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "taxi-events",
    bootstrap_servers="broker:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

taxis = {}

# punkty zakończenia kursów
demand_points = []

# poprzednia liczba kursów dla każdej taksówki
previous_rides = {}

message_counter = 0

print("Consumer started...")

for message in consumer:

    message_counter += 1

    event = message.value

    taxi_id = event["taxi_id"]

    # wykrywanie zakończenia kursu
    current_rides = event["completed_rides"]

    previous = previous_rides.get(
        taxi_id,
        current_rides
    )

    if current_rides > previous:

        demand_points.append(
            [
                event["lat"],
                event["lon"]
            ]
        )

    previous_rides[taxi_id] = current_rides

    # aktualizacja stanu taxi
    taxis[taxi_id] = event

    available = sum(
        1
        for taxi in taxis.values()
        if taxi["status"] == "available"
    )

    occupied = sum(
        1
        for taxi in taxis.values()
        if taxi["status"] == "occupied"
    )

    fleet_revenue = round(
        sum(
            taxi["earnings"]
            for taxi in taxis.values()
        ),
        2
    )

    completed_rides = sum(
        taxi["completed_rides"]
        for taxi in taxis.values()
    )

    top_taxis = sorted(
        taxis.values(),
        key=lambda x: x["earnings"],
        reverse=True
    )[:10]

    metrics = {

        "available_taxis": available,

        "occupied_taxis": occupied,

        "fleet_revenue": fleet_revenue,

        "completed_rides": completed_rides,

        "top_taxis": [
            {
                "taxi_id": taxi["taxi_id"],
                "earnings": taxi["earnings"],
                "distance_km": taxi["distance_km"],
                "completed_rides": taxi["completed_rides"]
            }
            for taxi in top_taxis
        ],

        "positions": [
            {
                "taxi_id": taxi["taxi_id"],
                "lat": taxi["lat"],
                "lon": taxi["lon"],
                "status": taxi["status"],
                "earnings": taxi["earnings"],
                "distance_km": taxi["distance_km"],
                "completed_rides": taxi["completed_rides"]
            }
            for taxi in taxis.values()
        ],

        # ostatnie 500 zakończeń kursów
        "demand_points": demand_points[-500:]
    }

    if message_counter % 100 == 0:

        with open("metrics.json", "w") as f:
            json.dump(metrics, f, indent=4)

        print(
            f"Available: {available} | "
            f"Occupied: {occupied} | "
            f"Revenue: {fleet_revenue} zł | "
            f"Demand points: {len(demand_points)}"
        )
