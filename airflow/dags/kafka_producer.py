from airflow.models import Variable
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from datetime import datetime, timedelta
import time
import requests
import json

nyt_key=Variable.get("NYT_KEY")
marketstack_access_key=Variable.get("MARKETSTACK_ACCESS_KEY")

#################### use NYT data as a test
def create_nyt_url(stock_name,page=1):
    print(nyt_key)
    return "https://api.nytimes.com/svc/search/v2/articlesearch.json?q={}&api-key={}&facet_fields=type_of_material&page={}".format(stock_name,nyt_key,page)

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


# def create_marketstack_url(stock_name='NVDA'):
#     return 'http://api.marketstack.com/v1/eod?access_key={}&symbols={}'.format(marketstack_access_key,stock_name)


# def connect_to_marketstack_endpoint(url):
#     response = requests.request("GET", url)
#     print('code',response.status_code)
#     if response.status_code != 200:
#         raise Exception(
#             "Request returned an error: {} {}".format(
#                 response.status_code, response.text
#             )
#         )
#     return json.dumps(response.json(), indent=4, sort_keys=True) # return json data

def create_topic_if_not_exists(topic_name, bootstrap_servers='kafka1:29092'):
    # First, create a Kafka admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='admin_client'
    )
    
    # Check if the topic exists by attempting to consume from it
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        topics = consumer.topics()
        
        if topic_name in topics:
            print(f"Topic '{topic_name}' already exists.")
        else:
            # Define the new topic configuration
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            # Attempt to create the topic
            admin_client.create_topics(new_topics=[topic], validate_only=False)
            print(f"Topic '{topic_name}' created.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic_name}' already exists.")
    finally:
        # Close the consumer and admin client
        consumer.close()
        admin_client.close()

def fetch_and_send_data(producer, topic_name, data):
    print(data)

    if data != last_data:
        producer.send(topic_name, value = data)
        print(f"Sent new data to:'{topic_name}': {data}")
        last_data = data
    return last_data

if __name__ == '__main__':

    topic_name = 'etl_topic'
    nyt_url = create_nyt_url('nvidia',8)
    nyt_data = connect_to_nyt_endpoint(nyt_url)
    # create_topic = create_topic_if_not_exists(topic_name)
    producer = KafkaProducer(bootstrap_servers='kafka1:29092', value_serializer=lambda v: v.encode('utf-8'))  # Serialize plain string to bytes
    while True:
        last_data = fetch_and_send_data(topic_name, nyt_data)
        time.sleep(60)

    # close the producer
    producer.flush()
    producer.close()