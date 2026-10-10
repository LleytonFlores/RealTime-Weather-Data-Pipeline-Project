from datetime import datetime

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder.appName("WeatherPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# structure of the JSON sent by the producers
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

# read both topics from kafka
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather-manila,weather-tokyo")
    .option("startingOffsets", "earliest")
    .load()
)

# turn kafka value bytes into columns
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


# save every micro-batch into cassandra
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

# print to the terminal
console_query = (
    weather_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

spark.streams.awaitAnyTermination()
