from datetime import datetime
import pandas as pd
import re
import os
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.sensors.filesystem import FileSensor


INPUT_PATH = "/opt/airflow/data/input/input.csv"
PROCESSED_PATH = "/opt/airflow/data/processed/processed.csv"

processed_dataset = Dataset(PROCESSED_PATH)

default_args = {
    "owner": "you",
    "depends_on_past": False,
}

def is_file_empty():
    if os.path.getsize(INPUT_PATH) == 0:
        return "file_empty_bash"
    return "process_group"

def replace_nulls(ti=None):
    df = pd.read_csv(PROCESSED_PATH)
    df = df.fillna("-")
    df = df.replace("null", "-", regex=False)
    df.to_csv(PROCESSED_PATH, index=False)

def sort_by_date():
    df = pd.read_csv(INPUT_PATH)
    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")
        df = df.sort_values(by=["created_date"], ascending=False)
    df.to_csv(PROCESSED_PATH, index=False)

def clean_content():
    df = pd.read_csv(PROCESSED_PATH)
    if "content" in df.columns:
        df["content"] = df["content"].astype(str).apply((lambda s: re.sub(r"[^0-9A-Za-zА-Яа-яёЁ.,;:!?()\"'\\-\\s]", "", s)))
    df.to_csv(PROCESSED_PATH, index=False)

with DAG(
    dag_id="process_data_dag",
    start_date=datetime(2021, 1, 1),
    schedule=None,
    default_args=default_args,
    tags=["process_data_dag"],
) as dag:

    wait_file = FileSensor(
        task_id="wait_file",
        filepath=INPUT_PATH,
        poke_interval=10,
        timeout=60*60,
    )

    branch = BranchPythonOperator(
        task_id="check_if_empty",
        python_callable=is_file_empty,
    )

    file_empty = BashOperator(
        task_id="file_empty_bash",
        bash_command="echo 'File empty'"
    )

    with TaskGroup("process_group") as tg:
        t1 = PythonOperator(task_id="replace_nulls", python_callable=replace_nulls, outlets=[processed_dataset])
        t2 = PythonOperator(task_id="sort_by_date", python_callable=sort_by_date)
        t3 = PythonOperator(
            task_id="clean_content",
            python_callable=clean_content,
        )

        t2 >> t3 >> t1

    wait_file >> branch
    branch >> file_empty
    branch >> tg
