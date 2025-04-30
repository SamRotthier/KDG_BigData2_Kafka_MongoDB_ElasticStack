import json
import random
import time
import datetime

from kafka import KafkaProducer

broker_address = "localhost:29092"
topic = "Velo-Data"

# Type metingen
measurement_types = ["GELUID", "FIJNSTOF", "TEMPERATUUR"]

# Antwerpen in 4 zones (x = lon, y = lat), met een bijhorende range van de verschillende meetwaardes (per zone)
zones = {
    "NW": {"x": (4.38, 4.41), "y": (51.23, 51.25), "noise": (75, 95), "dust": (25, 55), "temperature": (5, 25)},
    "NE": {"x": (4.41, 4.44), "y": (51.23, 51.25), "noise": (55, 75), "dust": (45, 75), "temperature": (10, 31)},
    "SW": {"x": (4.38, 4.41), "y": (51.20, 51.23), "noise": (60, 90), "dust": (20, 50), "temperature": (3, 22)},
    "SE": {"x": (4.41, 4.44), "y": (51.20, 51.23), "noise": (50, 70), "dust": (15, 45), "temperature": (15, 24)},
}

class Event:
    def __init__(self, timestamp, location, measurement_type, value, zone):
        self.timestamp = timestamp
        self.location = location
        self.measurement_type = measurement_type
        self.value = value
        self.zone = zone

def generate_event():
    # Selecteren van de random zone en random meting
    zone = random.choice(list(zones.keys()))
    measurement_type = random.choice(measurement_types)
    zone_info = zones[zone]

    # Afronden van coördinaten
    x = round(random.uniform(*zone_info["x"]), 5)
    y = round(random.uniform(*zone_info["y"]), 5)

    # Genereren van random meetwaardes voor de juiste (eerder bepaalde) meting
    if measurement_type == "GELUID":
        value = random.randint(*zone_info["noise"])
    elif measurement_type == "FIJNSTOF":
        value = round(random.uniform(*zone_info["dust"]), 1)
    else:
        value = round(random.uniform(*zone_info["temperature"]), 1)

    return event(
        timestamp=datetime.datetime.now().isoformat(),
        location={"x": x, "y": y},
        measurement_type=measurement_type,
        value=value,
        zone=zone
    )

producer = KafkaProducer(
    bootstrap_servers=broker_address,
    key_serializer=str.encode,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Verzenden van het event
while True:
    event = generate_event()
    event_dict = {
        "timestamp": event.timestamp,
        "location": event.location,
        "measurement_type": event.measurement_type,
        "value": event.value,
        "zone": event.zone
    }
    producer.send(topic, key=event.measurement_type, value=event_dict)
    print("Send:", event_dict)
    time.sleep(1)  # Elke seconde (ipv minuut, voor demo)
