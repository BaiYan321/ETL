from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    default_args=default_args,
    dag_id='dag_spark',
    description='Use airflow to interact with local spark',
    start_date=datetime(2024, 9, 12),
    schedule_interval='@daily'
) as dag:
    
    submit_job = SparkSubmitOperator(
        task_id='spark_job',
        application='spark_example.py',
        conf={
        "spark.executorEnv.JAVA_HOME": "C:\\Progra~2\\Java\\jdk1.8.0_202"
            },
        conn_id='spark_default',  # Ensure this is correctly configured in Airflow
        total_executor_cores=1,  # Integer instead of string
        executor_memory='1g',
        num_executors=1,
        driver_memory='1g',
        verbose=True
)

    submit_job
