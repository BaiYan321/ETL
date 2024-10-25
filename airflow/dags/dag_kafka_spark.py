from airflow.decorators import task
from airflow.models import Variable
from airflow import DAG
from kafka import KafkaProducer
from datetime import datetime, timedelta
import requests
import json

nyt_key=Variable.get("NYT_KEY")

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

#################### use NYT data as a test

with DAG(
    default_args=default_args,
    dag_id='extraction_streaming_processing',
    description='Use api to extract data with Kafka and process data with Spark',
    start_date=datetime(2024, 10, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    @task()
    def create_nyt_url():
        print(nyt_key)
        return "https://api.nytimes.com/svc/mostpopular/v2/viewed/1.json?api-key={}".format(nyt_key)

    @task()
    def connect_to_nyt_endpoint(url):
        response = requests.request("GET", url)
        print('code',response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return json.dumps(response.json(), indent=4, sort_keys=True) # return json data

    @task
    def fetch_and_send_data(topic_name, data):
        kafka_producer = KafkaProducer(bootstrap_servers='kafka1:29092', value_serializer=lambda v: v.encode('utf-8'))
        kafka_producer.send('etl_topic', value=data)
        # Close the producer
        kafka_producer.flush()
        kafka_producer.close()
        print(f"Sent new data to:'{topic_name}'")
    
    nyt_url = create_nyt_url()
    nyt_data = connect_to_nyt_endpoint(nyt_url)
    kafka_data = fetch_and_send_data('etl_topic', nyt_data)
    
    nyt_url >> nyt_data >> kafka_data
