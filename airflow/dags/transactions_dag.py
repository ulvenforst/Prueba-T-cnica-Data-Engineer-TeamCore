from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Añadir el directorio padre al path para importar módulos
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from airflow.scripts.extract import download_file
from airflow.scripts.transform import transform_data
from airflow.scripts.load import load_to_sqlite
from airflow.scripts.validate import validate_pipeline
from config import CSV_FILE_PATH

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='transactions_pipeline',
    default_args=default_args,
    description='Pipeline ETL para procesar transacciones',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'transactions', 'sqlite'],
) as dag:

    # 1. Sensor: esperar archivo (opcional para datos ya existentes)
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=str(CSV_FILE_PATH),
        poke_interval=30,
        timeout=300,  # 5 minutos máximo
        mode='poke'
    )

    # 2. Descarga/Verificación
    download = PythonOperator(
        task_id='download_file',
        python_callable=download_file
    )

    # 3. Transformación
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    # 4. Carga a SQLite
    load = PythonOperator(
        task_id='load_to_sqlite',
        python_callable=load_to_sqlite
    )

    # 5. Validación final
    validate = PythonOperator(
        task_id='validate_pipeline',
        python_callable=validate_pipeline
    )

    # Definir dependencias del DAG
    wait_for_file >> download >> transform >> load >> validate

    wait_for_file >> download >> transform >> load

