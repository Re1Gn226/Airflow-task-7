from datetime import datetime
import pandas as pd

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_PATH = "/opt/airflow/data/processed/processed.csv"
processed_dataset = Dataset(PROCESSED_PATH)

default_args = {
    "owner": "you",
}

def load_to_mongo():
    df = pd.read_csv(PROCESSED_PATH)
    mongo_hook = MongoHook(conn_id="mongo_default")
    client = mongo_hook.get_conn()
    db = client["airflow_data"]
    coll = db["processed_comments"]
    records = df.to_dict(orient="records")
    if records:
        coll.insert_many(records)
    client.close()

with DAG(
    dag_id="load_to_mongo_consumer",
    start_date=datetime(2025, 1, 1),
    schedule=[processed_dataset],
    catchup=False,
    default_args=default_args,
    tags=["consumer"],
) as dag:
    t = PythonOperator(task_id="load_to_mongo", python_callable=load_to_mongo)
