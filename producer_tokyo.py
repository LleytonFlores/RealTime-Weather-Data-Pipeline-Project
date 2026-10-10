import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# connect to local Kafka server
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "weather-tokyo"
city_name = "Tokyo, Japan"

# Open-Meteo API settings
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 35.6762,
    "longitude": 139.6503,
    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m",
    "timezone": "Asia/Tokyo"
}

print(f"Producer 2 started: {city_name} -> {topic_name}\n")

# fetch and send every 60 seconds
while True:
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        current = response.json()["current"]

        # format message as JSON
        message = {
            "name": city_name,
            "time": current["time"],
            "temperature": current["temperature_2m"],
            "apparent_temperature": current["apparent_temperature"],
            "humidity": current["relative_humidity_2m"],
            "is_day": current["is_day"],
            "precipitation": current["precipitation"],
            "cloud_cover": current["cloud_cover"],
            "pressure": current["pressure_msl"],
            "windspeed": current["wind_speed_10m"],
            "wind_direction": current["wind_direction_10m"],
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")
        }

        # send message
        producer.send(topic_name, message)
        producer.flush()
        print(f"Sent to {topic_name}: {message}")

    except requests.RequestException as e:
        print(f"Failed to fetch weather data: {e}")

    time.sleep(60)
