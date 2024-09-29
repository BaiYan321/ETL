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
        application='./dags/spark_example.py', # it works
        conn_id='sparkdefault',  # Make sure the 'sparkdefault' connection is correctly set up
        total_executor_cores=1,
        executor_memory='1g',
        num_executors=1,
        driver_memory='1g',
        verbose=True,
        conf={
             # It doesn't work becuase it's local path "spark.executorEnv.JAVA_HOME": "C:\\Progra~2\\Java\\jdk1.8.0_202"
            # "spark.executorEnv.JAVA_HOME": "/usr/lib/jvm/java-11-openjdk-amd64"
        #    "spark.executorEnv.JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-amd64"
            "spark.executorEnv.JAVA_HOME":"/usr/lib/jvm/java-17-openjdk-amd64"
    }
)

    submit_job
