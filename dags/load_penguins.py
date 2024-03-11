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


def load_data():
    df = pd.read_csv("data/penguins_lter.csv")

    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="taller3",
        password="taller3",
        database="penguins"
    )
    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE IF NOT EXISTS penguins (
            studyname VARCHAR(10),
            sample INT,
            species VARCHAR(100),
            region VARCHAR(20),
            island VARCHAR(20),
            stage VARCHAR(100),
            individual_id VARCHAR(10),
            clutch_completion VARCHAR(10),
            date_egg VARCHAR(100),
            culmen_length FLOAT,
            culmen_depth FLOAT,
            flipper_length INT,
            body_mass INT,
            sex VARCHAR(7),
            delta_15_n FLOAT,
            delta_13_c FLOAT,
            comments FLOAT
        )
    """

    cursor.execute(create_table_query)

    for _, row in df.iterrows():
        insert_query = """
            INSERT INTO penguins (studyname, sample, species, region, island, stage, individual_id, clutch_completion, date_egg, culmen_length, culmen_depth, flipper_length, body_mass, sex, delta_15_n, delta_13_c, comments)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = (row['studyname'], row['sample'], row['species'], row['region'], row['island'], row['stage'], row['individual_id'], row['clutch_completion'], row['date_egg'], row['culmen_length'], row['culmen_depth'], row['flipper_length'], row['body_mass'], row['sex'], row['delta_15_n'], row['delta_13_c'], row['comments'])
        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()


with DAG('load_data', default_args=default_args, schedule_interval="@once") as dag:
    load_csv_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    load_csv_task
