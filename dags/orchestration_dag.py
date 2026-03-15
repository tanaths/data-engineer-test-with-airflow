from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

def run_ingestion():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import ingestion2
    ingestion2.run()

def run_transformation():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import transformation2
    transformation2.run_all()

def run_metadata():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import metadata_upload
    metadata_upload.run()

def run_datamart():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))
    import datamart2
    datamart2.run()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ingestion_pipeline',
    default_args=default_args,
    description='Ingest manual file ke MySQL',
    schedule_interval='@daily',
    start_date=datetime(2026,3,14),
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_all_tables',
        python_callable=run_ingestion
    )

    metadata_upload_task = PythonOperator(
        task_id='upload_metadata',
        python_callable=run_metadata
    ) 

    transformation_task = PythonOperator(
        task_id='transformation_data',
        python_callable=run_transformation
    )

    datamart_task = PythonOperator(
        task_id='create_datamart',
        python_callable=run_datamart
    )

    ingest_task >> metadata_upload_task >> transformation_task >> datamart_task