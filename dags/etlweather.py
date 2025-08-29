# dags/etlweather.py
from airflow import DAG

# Safe import for get_current_context across Airflow versions (not required now, but kept)
try:
    from airflow.operators.python import get_current_context  # Airflow 2.x canonical
except Exception:  # pragma: no cover
    from airflow.decorators import get_current_context

from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
import time
import math
import os

# ------------------------ Config (env-overridable) ------------------------
LATITUDE = os.getenv("LATITUDE", "43.828863573253265")
LONGITUDE = os.getenv("LONGITUDE", "-79.29152494821345")
API_CONN_ID = os.getenv("API_CONN_ID", "open_meteo_api")
POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_default")

# How long to collect per run (seconds) and how often to sample (seconds)
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "120"))          # 2 minutes by default
SAMPLE_EVERY_SECONDS = int(os.getenv("SAMPLE_EVERY_SECONDS", "10"))

# Optional: schema to write into (defaults to Postgres 'public')
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

default_args = {
    "owner": "balaji",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 8, 14),  # fixed date
}

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    schedule="*/2 * * * *",  # every 2 minutes batch process
    catchup=False,
    tags=["weather", "batch", "sampling"],
) as dag:

    @task()
    def collect_samples_now() -> list[dict]:
        """
        Collect samples starting NOW for WINDOW_SECONDS, every SAMPLE_EVERY_SECONDS.
        This avoids the 'interval in the past' problem and guarantees rows on both
        manual and scheduled runs.
        """
        http = HttpHook(http_conn_id=API_CONN_ID, method="GET")
        endpoint = (
            f"/v1/forecast?latitude={LATITUDE}"
            f"&longitude={LONGITUDE}&current_weather=true"
        )

        def get_current():
            resp = http.run(endpoint)
            if resp.status_code != 200:
                return None
            payload = resp.json()
            return payload.get("current_weather") or payload.get("current")

        now_utc = datetime.now(timezone.utc)
        window_end = now_utc + timedelta(seconds=WINDOW_SECONDS)

        samples: list[dict] = []

        # Take an immediate sample
        current = get_current()
        if current:
            samples.append({
                "batch_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                "sampled_at": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
                "latitude": float(LATITUDE),
                "longitude": float(LONGITUDE),
                "temperature": float(current.get("temperature")),
                "windspeed": float(current.get("windspeed")),
                "winddirection": float(current.get("winddirection")),
                "weathercode": int(current.get("weathercode")),
            })

        # Then sample every SAMPLE_EVERY_SECONDS until window_end
        tick = max(1, SAMPLE_EVERY_SECONDS)
        next_ts = math.ceil(time.time() / tick) * tick

        while True:
            if datetime.fromtimestamp(next_ts, tz=timezone.utc) >= window_end:
                break

            delay = max(0.0, next_ts - time.time())
            if delay > 0.01:
                time.sleep(delay)

            sampled_at = datetime.now(timezone.utc)
            current = get_current()
            if current:
                samples.append({
                    "batch_end": window_end.strftime("%Y-%m-%d %H:%M:%S"),
                    "sampled_at": sampled_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "latitude": float(LATITUDE),
                    "longitude": float(LONGITUDE),
                    "temperature": float(current.get("temperature")),
                    "windspeed": float(current.get("windspeed")),
                    "winddirection": float(current.get("winddirection")),
                    "weathercode": int(current.get("weathercode")),
                })

            next_ts += tick

        print(f"[collect_samples_now] Collected {len(samples)} rows "
              f"(window={WINDOW_SECONDS}s, tick={SAMPLE_EVERY_SECONDS}s)")
        return samples

   
    @task()
    def load_to_postgres(rows: list[dict]) -> int:
        if not rows:
            print("[load_to_postgres] No rows to insert.")
            return 0

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        # optional: schema
        if POSTGRES_SCHEMA:
            cur.execute(f"SET search_path TO {POSTGRES_SCHEMA};")

        # ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude       DOUBLE PRECISION,
                longitude      DOUBLE PRECISION,
                temperature    DOUBLE PRECISION,
                windspeed      DOUBLE PRECISION,
                winddirection  DOUBLE PRECISION,
                weathercode    INTEGER
            );
        """)
        # --- schema migrations for older tables ---
        cur.execute("ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS observed_at TIMESTAMP;")
        cur.execute("ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS sampled_at  TIMESTAMP;")
        cur.execute("ALTER TABLE weather_data ADD COLUMN IF NOT EXISTS inserted_at TIMESTAMP DEFAULT NOW();")

        # helpful indexes (idempotent)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_observed_at ON weather_data(observed_at);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_weather_sampled_at  ON weather_data(sampled_at);")

        vals = [
            (
                r["latitude"], r["longitude"], r["temperature"], r["windspeed"],
                r["winddirection"], r["weathercode"], r["batch_end"], r["sampled_at"]
            )
            for r in rows
        ]

        cur.executemany(
            """
            INSERT INTO weather_data
            (latitude, longitude, temperature, windspeed, winddirection, weathercode, observed_at, sampled_at)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            vals,
        )
        conn.commit()
        cur.close()
        print(f"[load_to_postgres] Inserted {len(vals)} rows.")
        return len(vals)



    rows = collect_samples_now()
    inserted = load_to_postgres(rows)
