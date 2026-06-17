"""
Weather ETL Pipeline

Pulls current weather conditions for a configured location from the
Open-Meteo API on a daily schedule and persists each reading to PostgreSQL.

Pipeline stages:
    1. Extract   — fetch JSON from the Open-Meteo /forecast endpoint via HttpHook
    2. Transform — flatten the nested response into a single-level record
    3. Load      — insert the record into the `weather_data` Postgres table
"""

from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import pendulum

# Target location: Mumbai, India
LATITUDE = "19.0760"
LONGITUDE = "72.8774"

POSTGRES_CONN_ID = "postgres"
API_CONN_ID = "open_meteo_api"

default_args = {
    "owner": "airflow",
    "start_date": pendulum.now().subtract(days=1),
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    @task()
    def extract_weather_data() -> dict:
        """Fetch current weather from Open-Meteo via the configured HttpHook connection."""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = (
            f"/v1/forecast"
            f"?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        )
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()

        raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task()
    def transform_weather_data(weather_data: dict) -> dict:
        """Flatten the nested API response into a flat record ready for storage."""
        current = weather_data["current_weather"]
        return {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "winddirection": current["winddirection"],
            "weathercode": current["weathercode"],
        }

    @task()
    def load_weather_data(transformed_data: dict) -> None:
        """Insert a transformed weather record into the PostgreSQL weather_data table."""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude      FLOAT,
                longitude     FLOAT,
                temperature   FLOAT,
                windspeed     FLOAT,
                winddirection FLOAT,
                weathercode   INT,
                timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        cursor.execute(
            """
            INSERT INTO weather_data
                (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                transformed_data["latitude"],
                transformed_data["longitude"],
                transformed_data["temperature"],
                transformed_data["windspeed"],
                transformed_data["winddirection"],
                transformed_data["weathercode"],
            ),
        )

        conn.commit()
        cursor.close()

    # Wire up the pipeline: Extract -> Transform -> Load
    raw = extract_weather_data()
    transformed = transform_weather_data(raw)
    load_weather_data(transformed)
