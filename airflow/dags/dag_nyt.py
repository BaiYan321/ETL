from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv
#import os

default_args = {
    'owner': 'Ryan',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

nyt_key=Variable.get("NYT_KEY")

#使用decorator里的dag
@dag(dag_id='dag_with_nyt_api',
    description='Our first dag with api',
    default_args=default_args,
    start_date=datetime(2024, 9, 20),
    catchup=False,
    schedule_interval='@daily')

def nyt_etl():

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
        #下面的代码不运行
        #nyt_data = json.dumps(response, indent=4, sort_keys=True)
        #with open("nyt.json", "w") as outfile:
        #    outfile.write(nyt_data)
        #return

        #json.dumps(response.json(), indent=4, sort_keys=True)

        with open('nyt.json', 'w', encoding='utf-8') as f:
            json.dump(response.json(), f, ensure_ascii=False, indent=4,sort_keys=True)
        return
        
        
    url = create_nyt_url('nvidia',8)
    connect_to_nyt_endpoint(url)

nyt_data=nyt_etl()
