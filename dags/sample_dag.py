from datetime import datetime

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Extract weather data
def extract():
    print("Extracting weather data.")
    return {
        "date": str(datetime.today()),
        "location": "MAS",
        "weather": {
            "temp": np.random.randint(0, 40),
            "rain": bool(np.random.randint(0, 2)),
        },
    }


def transform(raw_data):
    transformed_data = [
        [
            raw_data["date"],
            raw_data.get("weather").get("temp"),
            raw_data.get("weather").get("rain"),
        ]
    ]

    return transformed_data


def load(data):
    df = pd.DataFrame(data, columns=["date", "temperature", "rain"])
    print(df)
    return df.values.tolist()


with DAG(
    dag_id="first_dag",
    start_date=datetime(2026, 1, 1, 9, 0),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:
    extract_data = PythonOperator(
        dag=dag, task_id="extract_data", python_callable=extract
    )

    transform_data = PythonOperator(
        dag=dag,
        task_id="transform_data",
        python_callable=transform,
        op_kwargs={"raw_data": "{{ti.xcom_pull(task_ids='extract_data')}}"},
    )

    load_data = PythonOperator(
        dag=dag,
        task_id="load_data",
        python_callable=load,
        op_kwargs={"data": "{{ti.xcom_pull(task_ids='transform_data')}}"},
    )

    extract_data >> transform_data >> load_data
