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
            f"FROM {TABLE} WHERE name = %s LIMIT 200",
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
