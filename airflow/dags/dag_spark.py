from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2023, 1, 1),
}

with DAG('spark_job', default_args=default_args, schedule_interval=None, catchup=False,) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/airflow/example.py',  # Path to your Spark job
        conn_id='spark_default',
        name='example_spark_job',
        application_args=[],
        conf={'spark.master': 'spark://spark-master:7077'},  # Spark Master URL
        executor_cores=1,
        executor_memory='1g',
        num_executors=1,
        verbose=True,
    )

    submit_spark_job
