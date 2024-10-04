from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

nyt_key=Variable.get("NYT_KEY")
marketstack_access_key=Variable.get("MARKETSTACK_ACCESS_KEY")

@dag(dag_id='dag_with_both_api',
    description='Dag with both NYT and MarketStack api',
    default_args=default_args,
    start_date=datetime(2024, 9, 20),
    catchup=False,
    schedule_interval='@daily')

def extraction_api():

    @task() #每个task之前都要声明
    def create_nyt_url(stock_name,page=1):
        print(nyt_key)
        return "https://api.nytimes.com/svc/search/v2/articlesearch.json?q={}&api-key={}&facet_fields=type_of_material&page={}".format(stock_name,nyt_key,page)
    
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

        return json.dumps(response.json(), indent=4, sort_keys=True)
        
    

    ##########################################################################

    @task() #每个task之前都要声明
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
    
    @task
    def final_task(result_a, result_b):
        return {'nyt': result_a, 'marketstack': result_b}
        
    nyt_url = create_nyt_url('nvidia',8)
    nyt_data = connect_to_nyt_endpoint(nyt_url)

    marketstack_url = create_marketstack_url('NVDA')
    marketstack_data = connect_to_marketstack_endpoint(marketstack_url)

    final_task(nyt_data, marketstack_data)

result = extraction_api()


