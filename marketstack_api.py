import os
from dotenv import load_dotenv

from datetime import datetime
import requests
import json

# load API key
load_dotenv()
marketstack_access_key=os.getenv("MARKETSTACK_ACCESS_KEY")

today=datetime.today().strftime('%Y-%m-%d')

def get_params(date_from="2020-01-01", date_to=today):
    return {"date_from":date_from, "date_to": date_to, "sort": "DESC"}


def create_url(stock_name='AAPL'):
    return "http://api.marketstack.com/v1/eod?access_key={}&symbols={}".format(marketstack_access_key,stock_name)


def connect_to_endpoint(url,params):
    response = requests.request("GET", url, params=params)
    print('code',response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


url = create_url()
params = get_params()
json_response = connect_to_endpoint(url,params)

stock_price = json.dumps(json_response, indent=4, sort_keys=True)
print(stock_price) #stock price is what we need



