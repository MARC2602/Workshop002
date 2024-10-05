from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from bd__process import load_data_from_db
from csv_process import process_csv
from merge_process import merge_data, load_to_db, upload_to_drive

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 5),  # Ajusta la fecha de inicio según sea necesario
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    description='DAG para procesamiento de datos de Spotify y premios Grammy',
    schedule_interval='@daily',  # Ajusta la frecuencia según sea necesario
)

# Task para leer datos de la base de datos
read_db = PythonOperator(
    task_id='read_db',
    python_callable=load_data_from_db,
    dag=dag,
)

# Task para transformar datos de la base de datos
transform_db = PythonOperator(
    task_id='transform_db',
    python_callable=save_to_db,
    op_args=[read_db.output],
    dag=dag,
)

# Task para leer datos del CSV
read_csv = PythonOperator(
    task_id='read_csv',
    python_callable=process_csv,
    dag=dag,
)

# Task para fusionar los datos
merge_data_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_data,
    op_args=[read_csv.output, transform_db.output],
    dag=dag,
)

# Task para cargar los datos en la base de datos
load_data_task = PythonOperator(
    task_id='load_to_db',
    python_callable=load_to_db,
    op_args=[merge_data_task.output],
    dag=dag,
)

# Task para subir a Google Drive
upload_data_task = PythonOperator(
    task_id='upload_to_drive',
    python_callable=upload_to_drive,
    op_args=[merge_data_task.output, '1o_7Fdw4s-Yhv_bp99vNamVY8t1v1ny79'],  # ID de tu carpeta en Google Drive
    dag=dag,
)

# Definición del flujo de tareas
read_db >> transform_db >> read_csv >> merge_data_task >> load_data_task >> upload_data_task
