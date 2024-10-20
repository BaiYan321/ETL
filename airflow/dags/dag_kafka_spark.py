from airflow.decorators import dag, task
from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from datetime import datetime, timedelta
import time
import requests
import json
import subprocess

nyt_key=Variable.get("NYT_KEY")
marketstack_access_key=Variable.get("MARKETSTACK_ACCESS_KEY")

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def run_kafka_producer():
    subprocess.run(["python", "kafka_producer.py"])

with DAG(
    default_args=default_args,
    dag_id='extraction_streaming_processing',
    description='Use api to extract data with Kafka and process data with Spark',
    start_date=datetime(2024, 10, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # #################### use NYT data as a test
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
    #     return json.dumps(response.json(), indent=4, sort_keys=True) # return json data
    # # @task()
    # # def create_marketstack_url(stock_name='NVDA'):
    # #     return 'http://api.marketstack.com/v1/eod?access_key={}&symbols={}'.format(marketstack_access_key,stock_name)
    
    # # @task()
    # # def connect_to_marketstack_endpoint(url):
    # #     response = requests.request("GET", url)
    # #     print('code',response.status_code)
    # #     if response.status_code != 200:
    # #         raise Exception(
    # #             "Request returned an error: {} {}".format(
    # #                 response.status_code, response.text
    # #             )
    # #         )
    # #     return json.dumps(response.json(), indent=4, sort_keys=True) # return json data
    
    # @task
    # def create_topic_if_not_exists(topic_name, bootstrap_servers='kafka1:29092'):
    #     # First, create a Kafka admin client
    #     admin_client = KafkaAdminClient(
    #         bootstrap_servers=bootstrap_servers,
    #         client_id='admin_client'
    #     )
        
    #     # Check if the topic exists by attempting to consume from it
    #     try:
    #         consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    #         topics = consumer.topics()
            
    #         if topic_name in topics:
    #             print(f"Topic '{topic_name}' already exists.")
    #         else:
    #             # Define the new topic configuration
    #             topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
    #             # Attempt to create the topic
    #             admin_client.create_topics(new_topics=[topic], validate_only=False)
    #             print(f"Topic '{topic_name}' created.")
    #     except TopicAlreadyExistsError:
    #         print(f"Topic '{topic_name}' already exists.")
    #     finally:
    #         # Close the consumer and admin client
    #         consumer.close()
    #         admin_client.close()

    # last_data = None
    # @task # kafka producer
    # def kafka_producer(topic_name, data):
    #     print(data)
    #     producer = KafkaProducer(
    #         bootstrap_servers='kafka1:29092',
    #         value_serializer=lambda v: v.encode('utf-8')  # Serialize plain string to bytes
    #             )
    #     while True:
    #         if data != last_data:
    #             producer.send(topic_name, value = data)
    #             print(f"Sent new data to:'{topic_name}': {data}")
    #             last_data = data
    #             time.sleep(60)
    #     # close the producer
    #     producer.flush()
    #     producer.close()

    kafka_producer_task = PythonOperator(
        task_id='kafka_producer',
        python_callable=run_kafka_producer
    )

    # submit_job = SparkSubmitOperator(
    #     task_id='spark_kafka_job',
    #     application='./dags/spark_processing.py', # it works
    #     conn_id='spark_default',  # Make sure the 'sparkdefault' connection is correctly set up
    #     total_executor_cores=2,
    #     executor_memory='2g',
    #     num_executors=2,
    #     driver_memory='1g',
    #     verbose=True,
    #     conf={
    #         'spark.dynamicAllocation.enabled': 'true',
    #         "spark.executorEnv.JAVA_HOME":"/usr/lib/jvm/java-17-openjdk-amd64"
    #         },
    # )

    # submit_job = SparkSubmitOperator(
    #     task_id='spark_job',
    #     application='./dags/spark_processing.py', # it works
    #     conn_id='spark_default',  # Make sure the 'sparkdefault' connection is correctly set up
    #     total_executor_cores=2,
    #     executor_memory='2g',
    #     num_executors=2,
    #     driver_memory='1g',
    #     # files='/data/marketstack.json',
    #     verbose=True,
    #     conf={
    #         'spark.dynamicAllocation.enabled': 'true',
    #         'spark.executorEnv.JAVA_HOME': '/opt/bitnami/java',
    #         'spark.driverEnv.JAVA_HOME': '/opt/bitnami/java'
    #         # 'spark.jars': '/data/clickhouse-jdbc.jar'
    #         },
    #     packages='org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0'
    # )


    topic_name = 'etl_topic'
    
    #nyt_url = create_nyt_url('nvidia',8)
    #nyt_data = connect_to_nyt_endpoint(nyt_url)
    # create_topic = create_topic_if_not_exists(topic_name)
    #streaming_data = kafka_producer("nyt_data", topic_name)

    # marketstack_url = create_marketstack_url('NVDA')
    # marketstack_data = connect_to_marketstack_endpoint(marketstack_url)
    # streaming_data = kafka_producer(marketstack_data)

    # make_directory >> marketstack_url >> marketstack_data >> streaming_data >> submit_job
    # make_directory >> nyt_url >> nyt_data  >> streaming_data >> submit_job
    # kafka_producer_task >> submit_job
    kafka_producer_task
