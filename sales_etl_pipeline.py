from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage, bigquery
import pandas as pd

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para ventas con Airflow y BigQuery',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Función de extracción
def extract_data():
    print("Extrayendo datos...")
    data = {
        "date": ["2024-11-01", "2024-11-01", "2024-11-02"],
        "product": ["Product A", "Product B", "Product C"],
        "quantity": [10, 5, 8],
        "price": [100.0, 50.0, 30.0],
    }
    df = pd.DataFrame(data)
    df.to_csv('/tmp/raw_sales_data.csv', index=False)

# Función de transformación
def transform_data():
    df = pd.read_csv('/tmp/raw_sales_data.csv')
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')  # Asegura el formato YYYY-MM-DD
    df['total_revenue'] = df['quantity'] * df['price']
    df.to_csv('/tmp/transformed_sales_data.csv', index=False)
    print("Datos transformados y guardados.")

# Función para cargar el archivo transformado en GCS
def load_transformed_to_gcs():
    storage_client = storage.Client()
    bucket = storage_client.bucket("sales-data-bucket-2609")
    blob = bucket.blob("transformed_sales_data.csv")
    blob.upload_from_filename("/tmp/transformed_sales_data.csv")
    print("Datos transformados cargados a GCS.")

# Tarea para cargar datos en BigQuery
load_to_bq_task = BigQueryInsertJobOperator(
    task_id='load_to_bigquery',
    configuration={
        "load": {
            "sourceUris": ["gs://sales-data-bucket-2609/transformed_sales_data.csv"],
            "destinationTable": {
                "projectId": "proyecto-bigquery-demo",
                "datasetId": "sales_data",
                "tableId": "daily_sales",
            },
            "sourceFormat": "CSV",
            "writeDisposition": "WRITE_APPEND",
            "skipLeadingRows": 1,  # Ignora la primera fila de encabezados
        }
    },
    location="US",
    dag=dag,
)

# Definir tareas
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_transformed_gcs_task = PythonOperator(
    task_id='load_transformed_to_gcs',
    python_callable=load_transformed_to_gcs,
    dag=dag,
)

# Flujo del DAG
extract_task >> transform_task >> load_transformed_gcs_task >> load_to_bq_task
