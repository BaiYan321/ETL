# ingest data into postgre

import pandas as pd
from time import time
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime
import requests
import json

from nyt_api import create_nyt_url, connect_to_nyt_endpoint
from marketstack_api import get_marketstack_params, create_marketstack_url, connect_to_marketstack_endpoint

pg_user="root" # 与docker yaml文件里的db一致
pg_password="root"
pg_host="localhost"
pg_port=5432
pg_db="etl_project" #在postgre里新建的database的名字

pg_table="stock"

# load API key
load_dotenv()
nyt_key=os.getenv("NYT_KEY")
marketstack_access_key=os.getenv("MARKETSTACK_ACCESS_KEY")

stock_name=input('Please enter the stock name')

engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}') #( user name: password @ host: port / database schema)










# load API key
load_dotenv()
nyt_key=os.getenv("NYT_KEY")
