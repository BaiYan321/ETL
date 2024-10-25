from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import requests
import json

#nyt_key=Variable.get("NYT_KEY")
marketstack_access_key=Variable.get("MARKETSTACK_ACCESS_KEY")

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    default_args=default_args,
    dag_id='dag_extraction_processing',
    description='Use api to extract data and process data with spark',
    start_date=datetime(2024, 10, 15),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    # @task() #每个task之前都要声明
    # def create_nyt_url(stock_name,page=1):
    #     print(nyt_key)
    #     return "https://api.nytimes.com/svc/search/v2/articlesearch.json?q={}&api-key={}&facet_fields=type_of_material&page={}".format(stock_name,nyt_key,page)
    
    # @task()
    # def connect_to_nyt_endpoint(url):
    #     response = requests.request("GET", url)
    #     print('code',response.status_code)
    #     if response.status_code != 200:
    #         raise Exception(
    #             "Request returned an error: {} {}".format(
    #                 response.status_code, response.text
    #             )
    #         )
    #     with open('nyt.json', 'w', encoding='utf-8') as f:
    #         json.dump(response.json(), f, ensure_ascii=False, indent=4,sort_keys=True)
    #     return


    @task()
    def create_marketstack_url(stock_name='NVDA'):
        return 'http://api.marketstack.com/v1/eod?access_key={}&symbols={}'.format(marketstack_access_key,stock_name)
    
    @task()
    def connect_to_marketstack_endpoint(url):
        response = requests.request("GET", url)
        print('code',response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        with open('/data/marketstack.json', 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=4,sort_keys=True)
        return
    submit_job = SparkSubmitOperator(
        task_id='spark_job',
        application='./dags/spark_processing.py', # it works
        conn_id='spark_default',  # Make sure the 'sparkdefault' connection is correctly set up
        total_executor_cores=1,
        executor_memory='1g',
        num_executors=1,
        driver_memory='1g',
        files='/data/marketstack.json',
        verbose=True,
        conf={
            'spark.dynamicAllocation.enabled': 'true',
            'spark.executorEnv.JAVA_HOME': '/opt/bitnami/java',
            'spark.driverEnv.JAVA_HOME': '/opt/bitnami/java',
            'spark.jars': '/data/clickhouse-jdbc.jar'
            },

    )

    #nyt_url = create_nyt_url('nvidia',8)
    #nyt_data = connect_to_nyt_endpoint(nyt_url)

    marketstack_url = create_marketstack_url('NVDA')
    marketstack_data = connect_to_marketstack_endpoint(marketstack_url)

    marketstack_url >> marketstack_data >> submit_job
    #submit_job
