# Weather Pipeline: Project Guide

**Open-Meteo API → Kafka → Spark → Cassandra → Flask**

Group 7 · AM2 · Flores, Silvestre

> **Reading this file:** in VS Code, press `Cmd + Shift + V` (Mac) or `Ctrl + Shift + V` (Windows) for the formatted preview.

---

## Overview

```
Open-Meteo API
   ├── producer_manila.py ──► topic: weather-manila ──┐
   └── producer_tokyo.py  ──► topic: weather-tokyo  ──┤
                                                      ▼
                                   spark_weather_stream.py (Spark 4.2.0)
                                                      ▼
                                 Cassandra: streaming_lab.weather_events
                                                      ▼
                                       app.py (Flask webpage)
```

### Versions

| Tool | Version | Where it came from |
|---|---|---|
| Java | 17 | Homebrew |
| Python | 3.12 | Homebrew (no venv) |
| Kafka | 4.3.1 | FA2 |
| Spark | 4.2.0 (Scala 2.13) | FA3 |
| Cassandra | latest (Docker) | FA4 |
| Flask + cassandra-driver | latest | Cassandra + Flask notes |

### Files in this folder

| File | Part |
|---|---|
| `producer_manila.py` | Part 2: Producer 1 |
| `producer_tokyo.py` | Part 2: Producer 2 |
| `spark_weather_stream.py` | Part 4: Spark |
| `app.py` | Part 6: Flask |

### Progress

- [x] Prerequisites: Java, Python, Kafka, Spark
- [ ] Prerequisites: Docker screenshot
- [ ] Part 1: Data source
- [ ] Part 2: Producers
- [ ] Part 3: Topics
- [ ] Part 4: Spark
- [ ] Part 5: Cassandra
- [ ] Part 6: Flask
- [ ] Part 7: Screenshots
- [ ] Part 8: Video
- [ ] Part 9: Contributions

---

## Prerequisites

### Step P.1: Verify Java
```bash
java -version
```
**Expected:** version 17 or higher.

### Step P.2: Verify Python
```bash
python3 --version
which python3
```
**Expected:** `Python 3.12.x` from `/opt/homebrew/opt/python@3.12/libexec/bin/python3`.

### Step P.3: Verify Kafka
```bash
echo $KAFKA_HOME
$KAFKA_HOME/bin/kafka-topics.sh --version
```
**Expected:** `/Users/lleytonflores/kafka`, then `4.3.1`.

### Step P.4: Verify Spark
```bash
echo $SPARK_HOME
spark-submit --version
```
**Expected:** `/Users/lleytonflores/spark`, then `version 4.2.0`, Scala 2.13.

The `WARN Utils` lines about the hostname and loopback address are normal; FA3 showed them too.

### Step P.5: Verify Docker and the Cassandra container
```bash
docker --version
docker ps -a --filter name=cassandra
```
**Expected:** a Docker version, then a row named `cassandra`. It's the FA4 container, and it may say `Exited`; Part 5 starts it.

### Step P.6: Verify the Python packages
```bash
python3 -c "import kafka, requests, cassandra, flask; print('packages OK')"
```
**Expected:** `packages OK`. If it fails, run:
```bash
python3 -m pip install --break-system-packages kafka-python-ng requests cassandra-driver flask
```

**Screenshot:** each check (P.1 to P.6).

---

## Part 1: Live Data Source

**Source:** [Open-Meteo Weather API](https://open-meteo.com/en/docs). It's free and needs no API key. Its "current" weather values refresh about every 15 minutes.

### Step 1.1: Test the API in a browser

**Manila**
```
https://api.open-meteo.com/v1/forecast?latitude=14.5995&longitude=120.9842&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m&timezone=Asia/Manila
```

**Tokyo**
```
https://api.open-meteo.com/v1/forecast?latitude=35.6762&longitude=139.6503&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m&timezone=Asia/Tokyo
```

**Expected:** JSON with a `"current"` block.

### Step 1.2: Fields extracted

| # | Field sent to Kafka | From the API | Unit |
|---|---|---|---|
| 1 | `name` | set in the producer | – |
| 2 | `time` | `current.time` | local time |
| 3 | `temperature` | `current.temperature_2m` | °C |
| 4 | `apparent_temperature` | `current.apparent_temperature` | °C ("feels like") |
| 5 | `humidity` | `current.relative_humidity_2m` | % |
| 6 | `is_day` | `current.is_day` | 1 = day, 0 = night |
| 7 | `precipitation` | `current.precipitation` | mm (last 15 min) |
| 8 | `cloud_cover` | `current.cloud_cover` | % |
| 9 | `pressure` | `current.pressure_msl` | hPa |
| 10 | `windspeed` | `current.wind_speed_10m` | km/h |
| 11 | `wind_direction` | `current.wind_direction_10m` | degrees (0 = N, 90 = E) |
| + | `fetched_at` | producer's own clock (UTC) | timestamp |

`fetched_at` is when the producer called the API. Because the API only changes every ~15 minutes, `fetched_at` keeps every reading as its own row in Cassandra.

**Screenshot:** both URLs open in the browser.

---

## Part 2: Create Two Producers

Each producer calls the API every 60 seconds and sends one JSON message to its own topic.

### Step 2.1: Create `producer_manila.py`
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
nano producer_manila.py
```
```python
import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# Connect to local Kafka server
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = "weather-manila"
city_name = "Manila, Philippines"

# Open-Meteo API settings
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 14.5995,
    "longitude": 120.9842,
    "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,cloud_cover,pressure_msl,wind_speed_10m,wind_direction_10m",
    "timezone": "Asia/Manila"
}

print(f"Producer 1 started: {city_name} -> {topic_name}\n")

# Keep fetching and sending every 60 seconds
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
```

### Step 2.2: Create `producer_tokyo.py`
```bash
nano producer_tokyo.py
```
```python
import json
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

# Connect to local Kafka server
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

# Keep fetching and sending every 60 seconds
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
```

Don't run the producers yet. Kafka and the topics come first (Part 3).

### How this differs from the FA2 producer

| FA2 producer | Project producers |
|---|---|
| Ran once and sent 3 messages | `while True` sends every 60 seconds |
| Sent formatted text | Sends JSON (`json.dumps`) so Spark can split it into columns |
| Scraped HTML with BeautifulSoup | Reads JSON straight from the API |
| One producer, one topic | Two producers, two topics |

---

## Part 3: Create Two Kafka Topics

### Step 3.1: Start the Kafka server
**Terminal 1: KAFKA**
```bash
cd $KAFKA_HOME
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```
Only run the `format` command the first time. Later starts only need the last line.

**Expected:** `Kafka Server started` near the end. Leave this terminal running.

### Step 3.2: Create the topics
**Terminal 2: TOPICS**
```bash
cd $KAFKA_HOME
bin/kafka-topics.sh --create --topic weather-manila --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic weather-tokyo --bootstrap-server localhost:9092
```
**Expected:** `Created topic weather-manila.` and `Created topic weather-tokyo.`

### Step 3.3: Verify the topics
```bash
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic weather-manila --bootstrap-server localhost:9092
bin/kafka-topics.sh --describe --topic weather-tokyo --bootstrap-server localhost:9092
```

### Step 3.4: Show the topics are separate
Run the producers and watch each topic in its own consumer, the same way as FA2's consumers.

**Terminal 3: PRODUCER 1**
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
python3 producer_manila.py
```

**Terminal 4: PRODUCER 2**
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
python3 producer_tokyo.py
```

**Terminal 5: CONSUMER MANILA**
```bash
cd $KAFKA_HOME
bin/kafka-console-consumer.sh --topic weather-manila --bootstrap-server localhost:9092 --from-beginning
```

**Terminal 6: CONSUMER TOKYO**
```bash
cd $KAFKA_HOME
bin/kafka-console-consumer.sh --topic weather-tokyo --bootstrap-server localhost:9092 --from-beginning
```
**Expected:** the Manila consumer shows only Manila JSON, and the Tokyo consumer shows only Tokyo JSON.

Leave both producers running. You can close the two consumers (Ctrl+C) once you have screenshots.

**Screenshot:** Kafka running, topic creation, `--list`/`--describe`, each producer, each consumer.

---

## Part 4: Stream the Data with Spark

> **Before this part:** do Part 5, Steps 5.1–5.3. The Cassandra table must exist before Spark writes to it.

### Step 4.1: Create `spark_weather_stream.py`
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
nano spark_weather_stream.py
```
```python
from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("WeatherPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Structure of the JSON sent by the producers
schema = StructType([
    StructField("name", StringType()),
    StructField("time", StringType()),
    StructField("temperature", DoubleType()),
    StructField("apparent_temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("is_day", IntegerType()),
    StructField("precipitation", DoubleType()),
    StructField("cloud_cover", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("wind_direction", DoubleType()),
    StructField("fetched_at", StringType()),
])

# Read both topics from Kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather-manila,weather-tokyo")
    .option("startingOffsets", "earliest")
    .load()
)

# Turn the Kafka value bytes into columns
weather_df = (
    df.select(
        col("topic"),
        from_json(col("value").cast("string"), schema).alias("data")
    )
    .select(
        col("data.name").alias("name"),
        col("data.fetched_at").alias("fetched_at"),
        col("data.time").alias("observation_time"),
        col("data.temperature").alias("temperature"),
        col("data.apparent_temperature").alias("apparent_temperature"),
        col("data.humidity").alias("humidity"),
        col("data.is_day").alias("is_day"),
        col("data.precipitation").alias("precipitation"),
        col("data.cloud_cover").alias("cloud_cover"),
        col("data.pressure").alias("pressure"),
        col("data.windspeed").alias("windspeed"),
        col("data.wind_direction").alias("wind_direction"),
        col("topic")
    )
    .filter(col("name").isNotNull())
)

weather_df.printSchema()


# Save every micro-batch into Cassandra
def write_to_cassandra(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect("streaming_lab")
    insert = session.prepare(
        "INSERT INTO weather_events "
        "(name, fetched_at, observation_time, temperature, apparent_temperature, humidity, "
        "is_day, precipitation, cloud_cover, pressure, windspeed, wind_direction, topic) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    for row in rows:
        session.execute(insert, (
            row.name,
            datetime.fromisoformat(row.fetched_at),
            row.observation_time,
            row.temperature,
            row.apparent_temperature,
            row.humidity,
            row.is_day,
            row.precipitation,
            row.cloud_cover,
            row.pressure,
            row.windspeed,
            row.wind_direction,
            row.topic
        ))

    cluster.shutdown()
    print(f"Batch {batch_id}: saved {len(rows)} row(s) to Cassandra")


cassandra_query = (
    weather_df.writeStream
    .foreachBatch(write_to_cassandra)
    .outputMode("append")
    .option("checkpointLocation", "checkpoint")
    .start()
)

# Also print to the terminal like FA3
console_query = (
    weather_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

spark.streams.awaitAnyTermination()
```

### Step 4.2: Run the Spark job
**Terminal 7: SPARK**
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark_weather_stream.py
```
**Expected:**
- The schema prints first.
- Then `Batch: 0`, `Batch: 1`, … tables appear with Manila and Tokyo rows.
- Lines like `Batch 1: saved 2 row(s) to Cassandra` appear.

### Step 4.3: Check the Spark Web UI
Open **http://localhost:4040**. If it isn't there, try 4041, because a pyspark session may still be open (the same problem as FA3).

| Tab | What to show |
|---|---|
| Jobs | Completed jobs, one per batch |
| SQL / DataFrame | The queries for each batch |
| Structured Streaming | **2 active queries** (the Cassandra writer and the console), input rate, process rate |

**Screenshot:** the terminal batches and each tab.

---

## Part 5: Store the Data in Cassandra

### Step 5.1: Start the Cassandra container
**Terminal 8: CASSANDRA.** Open Docker Desktop first.
```bash
docker start cassandra
docker logs -f cassandra
```
Wait for `Startup complete`, then press Ctrl+C. Cassandra keeps running; Ctrl+C only stops the log view.
```bash
docker ps
```
**Expected:** `cassandra` with STATUS `Up ...`.

### Step 5.2: Open cqlsh and create the keyspace
```bash
docker exec -it cassandra cqlsh
```
```sql
CREATE KEYSPACE IF NOT EXISTS streaming_lab
WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor': 1};

USE streaming_lab;
```
**Expected:** the prompt changes to `cqlsh:streaming_lab>`.

### Step 5.3: Create the table
```sql
CREATE TABLE IF NOT EXISTS weather_events (
    name text,
    fetched_at timestamp,
    observation_time text,
    temperature double,
    apparent_temperature double,
    humidity double,
    is_day int,
    precipitation double,
    cloud_cover double,
    pressure double,
    windspeed double,
    wind_direction double,
    topic text,
    PRIMARY KEY ((name), fetched_at)
) WITH CLUSTERING ORDER BY (fetched_at DESC);

DESCRIBE TABLES;
DESCRIBE TABLE weather_events;
```

How the table columns match the extracted fields:

| Extracted field | Cassandra column | Type |
|---|---|---|
| name | `name` | text (partition key) |
| fetched_at | `fetched_at` | timestamp (clustering key, newest first) |
| time | `observation_time` | text (`time` is a CQL type name) |
| temperature | `temperature` | double |
| apparent_temperature | `apparent_temperature` | double |
| humidity | `humidity` | double |
| is_day | `is_day` | int (1 = day, 0 = night) |
| precipitation | `precipitation` | double |
| cloud_cover | `cloud_cover` | double |
| pressure | `pressure` | double |
| windspeed | `windspeed` | double |
| wind_direction | `wind_direction` | double |
| – | `topic` | text (which Kafka topic the row came from) |

### Step 5.4: Verify the stored rows (after Part 4 is running)
```sql
SELECT * FROM weather_events WHERE name = 'Manila, Philippines' LIMIT 10;
SELECT * FROM weather_events WHERE name = 'Tokyo, Japan' LIMIT 10;
SELECT COUNT(*) FROM weather_events;
```
**Expected:** rows for both cities, with the count growing every minute.

**Screenshot:** each step, and the SELECT results.

---

## Part 6: Display the Data in Flask

### Step 6.1: Create `app.py`
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
nano app.py
```
```python
from flask import Flask, render_template_string
from cassandra.cluster import Cluster

app = Flask(__name__)

KEYSPACE = "streaming_lab"
TABLE = "weather_events"
CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_PORT = 9042
CITIES = ["Manila, Philippines", "Tokyo, Japan"]

HTML_TEMPLATE = """
<!doctype html>
<html>
<head>
    <title>Live Weather Pipeline</title>
    <meta http-equiv="refresh" content="10">
    <style>
        body { font-family: Arial, sans-serif; margin: 30px; background: #f4f6f9; }
        .city { background: #fff; padding: 16px; margin-bottom: 24px; border-radius: 8px;
                box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
        .scroll { max-height: 400px; overflow-y: auto; border: 1px solid #ccc; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background: #2c3e50; color: white; position: sticky; top: 0; }
        tr:nth-child(even) { background: #fafafa; }
    </style>
</head>
<body>
    <h1>Live Weather Pipeline</h1>
    <p>Data from Cassandra keyspace <b>{{ keyspace }}</b>, table <b>{{ table }}</b>.
       This page refreshes every 10 seconds.</p>

    {% for city, rows in data.items() %}
    <div class="city">
        <h2>{{ city }} ({{ rows|length }} rows)</h2>
        {% if rows %}
        <div class="scroll">
            <table>
                <tr>
                    <th>fetched_at (UTC)</th>
                    <th>time</th>
                    <th>temperature (°C)</th>
                    <th>feels like (°C)</th>
                    <th>humidity (%)</th>
                    <th>day/night</th>
                    <th>precipitation (mm)</th>
                    <th>cloud cover (%)</th>
                    <th>pressure (hPa)</th>
                    <th>windspeed (km/h)</th>
                    <th>wind direction (°)</th>
                </tr>
                {% for row in rows %}
                <tr>
                    <td>{{ row.fetched_at }}</td>
                    <td>{{ row.observation_time }}</td>
                    <td>{{ row.temperature }}</td>
                    <td>{{ row.apparent_temperature }}</td>
                    <td>{{ row.humidity }}</td>
                    <td>{{ "Day" if row.is_day == 1 else "Night" }}</td>
                    <td>{{ row.precipitation }}</td>
                    <td>{{ row.cloud_cover }}</td>
                    <td>{{ row.pressure }}</td>
                    <td>{{ row.windspeed }}</td>
                    <td>{{ row.wind_direction }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>
        {% else %}
            <p>No rows found.</p>
        {% endif %}
    </div>
    {% endfor %}
</body>
</html>
"""

@app.route("/")
def home():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect(KEYSPACE)

    data = {}
    for city in CITIES:
        rows = session.execute(
            f"SELECT fetched_at, observation_time, temperature, apparent_temperature, humidity, "
            f"is_day, precipitation, cloud_cover, pressure, windspeed, wind_direction "
            f"FROM {TABLE} WHERE name = %s LIMIT 100",
            (city,)
        )
        data[city] = list(rows)

    cluster.shutdown()

    return render_template_string(
        HTML_TEMPLATE,
        data=data,
        keyspace=KEYSPACE,
        table=TABLE,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
```

### Step 6.2: Run Flask
**Terminal 9: FLASK**
```bash
cd ~/Documents/CurrentTerm/DataScience2/Weather-Pipeline
flask --app app run --host=0.0.0.0 --port 5001 --debug
```
Open **http://localhost:5001**. The Mac uses port 5001 because AirPlay takes 5000; Venus can use 5000.

**Expected:** two scrollable tables, Manila and Tokyo, with the newest rows on top. The row count goes up after each producer cycle, because the page refreshes every 10 seconds.

**Screenshot:** the page, and the page again a few minutes later with more rows.

---

## Part 7: Proof of the Workflow (screenshot checklist)

| Requirement | Screenshot from |
|---|---|
| Producer 1 | Terminal 3: `producer_manila.py` output |
| Producer 2 | Terminal 4: `producer_tokyo.py` output |
| Topics working | Terminal 2: `--list` / `--describe`, plus Terminals 5–6 consumers |
| Spark dashboard | localhost:4040, the Jobs, SQL and Structured Streaming tabs |
| Spark processing | Terminal 7: console batches and "saved to Cassandra" lines |
| Cassandra data | Terminal 8: `DESCRIBE TABLE` and `SELECT` results |
| Flask webpage | localhost:5001 |

---

## Part 8: Video Demonstration

### Start order
1. Docker Desktop → `docker start cassandra` (Part 5.1)
2. Kafka server (Part 3.1)
3. Spark job (Part 4.2)
4. Producer 1 and Producer 2 (Part 3.4)
5. Flask (Part 6.2)

### What the video must show
1. **Live source:** the Open-Meteo URLs in the browser, and the producers printing `Sent to ...`.
2. **Kafka receiving:** the console consumers on both topics.
3. **Spark processing:** terminal batches and the localhost:4040 Structured Streaming tab.
4. **Cassandra storing:** `SELECT` in cqlsh, run twice to show the count increasing.
5. **Flask output:** localhost:5001 refreshing with new rows.

Upload to YouTube as **Unlisted** (not Private) so the link is accessible, and put the link in the document.

---

## Part 9: Student Contribution

For each member, write in your own words:
- What part of the project they did
- What they learned

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `NoBrokersAvailable` (producer) | The Kafka server (Terminal 1) isn't running |
| Spark UI on 4041 instead of 4040 | Close the old pyspark/Spark session |
| `ModuleNotFoundError: cassandra` in Spark | Run `echo $PYSPARK_PYTHON`, which should be `python3.12`, then rerun the pip install from P.6 |
| `Unsupported class file major version` | The terminal is using old Java. Run `source ~/.zshrc` and check `java -version` |
| Spark errors after editing the script | Delete the `checkpoint` folder in this directory |
| `NoHostAvailable` (Spark or Flask) | Cassandra isn't up. Check `docker ps` and wait for `Startup complete` |
| Flask "Address already in use" on 5000 | Use `--port 5001` (macOS AirPlay uses 5000) |

---

## Venus on Windows (WSL Ubuntu)

- Use the **Ubuntu (WSL)** terminal, not PowerShell. Edit `~/.bashrc` instead of `~/.zshrc`.
- In Docker Desktop, turn on **Use WSL 2 based engine** and **WSL integration** for Ubuntu.
- Kafka, Spark 4.2.0 and Java from FA3 should already be installed. Check them with the Prerequisites steps.
- Python: Ubuntu's `python3` is already 3.12. Install the packages with:
  ```bash
  python3 -m pip install --break-system-packages kafka-python-ng requests cassandra-driver flask
  ```
- Project folder: `mkdir -p ~/Weather-Pipeline && cd ~/Weather-Pipeline`, then copy in the four `.py` files.
- Flask can use port 5000.
