# https://www.youtube.com/watch?v=K9AnJ9_ZAXE

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator #这里调用的是PythonOperator

import os
from dotenv import load_dotenv

import requests
import json

default_args = {
    'owner': 'YanBai',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

##########################           marketstack                ##################################

load_dotenv()
# marketstack_access_key=os.getenv("MARKETSTACK_ACCESS_KEY")
marketstack_access_key="89b1f6ae5016481cbcd3aa51153f136b"

today=datetime.today().strftime('%Y-%m-%d')

def get_marketstack_params(date_from="2020-01-01", date_to=today):
    return {"date_from":date_from, "date_to": date_to, "sort": "DESC"}

def create_marketstack_url(stock_name='NVDA'):
    return 'http://api.marketstack.com/v1/eod?access_key={}&symbols={}'.format(marketstack_access_key,stock_name)


def connect_to_marketstack_endpoint(u,p):
    # 好 connect_to_marketstack_endpoint():
    url = u.xcom_pull(task_ids='create_m_url')
    params = p.xcom_pull(task_ids='get_m_params')

    response = requests.request("GET", url,params=params)
    # 好 response = requests.request("GET", "http://api.marketstack.com/v1/eod?access_key=89b1f6ae5016481cbcd3aa51153f136b&symbols=NVDA")
    print('code',response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return json.dumps(response.json(), indent=4, sort_keys=True)

# 使用xcom的方法传递task之间的数据,为什么不work

##########################           nyt                ##################################
#nyt_key=os.getenv("NYT_KEY")
nyt_key="k03K6pbBIwWWbPvTpgCsUi8uGUSCG1Pt"

def create_nyt_url(stock_name,page=1):
    return "https://api.nytimes.com/svc/search/v2/articlesearch.json?q={}&api-key={}&facet_fields=type_of_material&page={}".format(stock_name,nyt_key,page)

def connect_to_nyt_endpoint(n):
    url=n.xcom_pull(task_ids="create_n_url")
    response = requests.request("GET", url)
    print('code',response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return json.dumps(response.json(), indent=4, sort_keys=True)


with DAG(
    default_args=default_args,
    dag_id='dag_api',
    description='Use API to get NYT data and stock price',
    start_date=datetime(2024, 9, 12),
    schedule_interval='@daily'
) as dag:

    task11 = PythonOperator(
        task_id='get_m_params',
        python_callable=get_marketstack_params
    )

    task12 = PythonOperator(
        task_id='create_m_url',
        python_callable=create_marketstack_url, 
        #airflow的Xcom里会显示这个返回的值
        op_kwargs={'stock_name': "NVDA"} 
    )

    task13 = PythonOperator(
        task_id='connect_to_m_endpoint',
        python_callable=connect_to_marketstack_endpoint 
    )

    task21 = PythonOperator(
        task_id='create_n_url',
        python_callable=create_nyt_url,
        op_kwargs={"stock_name": "NVDA"}
    )

    task22 = PythonOperator(
        task_id='connect_to_n_endpoint',
        python_callable=connect_to_nyt_endpoint
    )

    [task11, task12]>> task13 # 两个独立运行完，才进行下一个
    task21 >> task22
    
    