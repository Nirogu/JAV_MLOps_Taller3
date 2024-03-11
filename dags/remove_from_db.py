from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def truncate_penguins_table():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="taller3",
        password="taller3",
        database="penguins"
    )

    cursor = conn.cursor()

    truncate_query = "TRUNCATE TABLE penguins"
    cursor.execute(truncate_query)

    conn.commit()
    cursor.close()
    conn.close()


with DAG('truncate_penguins_table', default_args=default_args, schedule_interval="@once") as dag:
    truncate_task = PythonOperator(
        task_id='truncate_penguins_table',
        python_callable=truncate_penguins_table
    )

    truncate_task
