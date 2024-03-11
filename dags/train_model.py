from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, OrdinalEncoder, StandardScaler

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def train_model():
    conn = mysql.connector.connect(
        host="127.0.0.1",
        user="taller3",
        password="taller3",
        database="penguins"
    )
    query = "SELECT species, culmen_length, culmen_depth, flipper_length, body_mass, sex, delta_15_n, delta_13_c FROM penguins"
    data = pd.read_sql_query(query, conn)
    conn.close()

    new_names = [
        "specie",
        "culmenLen",
        "culmenDepth",
        "flipperLen",
        "bodyMass",
        "sex",
        "delta15N",
        "delta13C",
    ]
    data.columns = new_names

    # clean data
    data.replace({"sex": {".": None}}, inplace=True)
    data.dropna(inplace=True)

    # transform data
    target_variable = "specie"
    numerical_features = [
        "culmenLen",
        "culmenDepth",
        "flipperLen",
        "bodyMass",
        "delta15N",
        "delta13C",
    ]
    categorical_features = ["sex"]

    label_encoder = LabelEncoder().fit(data[target_variable])
    y = label_encoder.transform(data[target_variable])
    X = data.drop(columns=[target_variable])

    scaler = StandardScaler().fit(X[numerical_features])
    X[numerical_features] = scaler.transform(X[numerical_features])

    values_encoder = OrdinalEncoder().fit(X[categorical_features])
    X[categorical_features] = values_encoder.transform(X[categorical_features])

    # split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=1337
    )

    # build model
    lr_model = LogisticRegression().fit(X_train, y_train)

    # evaluate model
    print(f"Logistic regression: {lr_model.score(X_test, y_test):.2f} test accuracy")

    # save model
    with open("label_encoder.pkl", "wb") as pklfile:
        pickle.dump(label_encoder, pklfile, pickle.HIGHEST_PROTOCOL)
    with open("standard_scaler.pkl", "wb") as pklfile:
        pickle.dump(scaler, pklfile, pickle.HIGHEST_PROTOCOL)
    with open("variable_encoder.pkl", "wb") as pklfile:
        pickle.dump(values_encoder, pklfile, pickle.HIGHEST_PROTOCOL)
    with open("lr_model.pkl", "wb") as pklfile:
        pickle.dump(lr_model, pklfile, pickle.HIGHEST_PROTOCOL)


with DAG('train_model', default_args=default_args, schedule_interval="@once") as dag:
    truncate_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model
    )

    truncate_task
