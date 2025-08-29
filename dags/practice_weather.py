import os
import json
from pathlib import Path
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

# Read from env (with sane defaults)
LAT = os.getenv("LATITUDE", "43.828863573253265")
LON = os.getenv("LONGITUDE", "-79.29152494821345")
API_CONN_ID = os.getenv("API_CONN_ID", "open_meteo_api")

ENDPOINT = (
    f"/v1/forecast?latitude={LAT}"
    f"&longitude={LON}&current_weather=true"
)

# Save under include/data so it’s visible on your host.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "include" / "data" / "practice_weather.json"
def fetch_and_save():
    http = HttpHook(http_conn_id=API_CONN_ID, method="GET")
    res = http.run(ENDPOINT)
    res.raise_for_status()
    payload = res.json()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=4)
   

with DAG(
    dag_id="practice_weather",
    start_date=datetime(2025, 8, 1),
    schedule=None,   # e.g. "@hourly" to run hourly
    catchup=False,
    default_args={"owner": "balaji", "retries": 2},
    tags=["demo", "weather"],
) as dag:
    PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_and_save,
    )
