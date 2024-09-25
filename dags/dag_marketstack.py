
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv
import os

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

load_dotenv()
marketstack_access_key=os.getenv("MARKETSTACK_ACCESS_KEY")

#使用decorator里的dag
@dag(dag_id='dag_with_api_marketstack',
    description='Our first dag with api',
    default_args=default_args,
    start_date=datetime(2024, 9, 20),
    schedule_interval='@daily')

def marketstack_etl():
    
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
        #return
        return json.dumps(response.json(), indent=4, sort_keys=True)
    
    #params = get_marketstack_params()    
    url = create_marketstack_url('NVDA')
    connect_to_marketstack_endpoint(url)

#create instance of our dag.
marketstack_data = marketstack_etl()
