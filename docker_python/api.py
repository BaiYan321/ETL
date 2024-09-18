import os
from dotenv import load_dotenv

from datetime import datetime
import requests
import json

##############################     nyt         #######################################
# load API key
load_dotenv()
nyt_key=os.getenv("NYT_KEY")


def create_nyt_url(stock_name,page):
    return "https://api.nytimes.com/svc/search/v2/articlesearch.json?q={}&api-key={}&facet_fields=type_of_material&page={}".format(stock_name,nyt_key,page)


def connect_to_nyt_endpoint(url):
    response = requests.request("GET", url)
    print('code',response.status_code)
    print(response.json())
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json() 

######## Fetch all the content, if the page contains 10 articles, then we continue the next page   ############

url = create_nyt_url('nvidia',10)
json_response = connect_to_nyt_endpoint(url)

nyt_data = json.dumps(json_response, indent=4, sort_keys=True)

# save as json file
with open("nyt.json", "w") as outfile:
    outfile.write(nyt_data)

##############################     marketstack         #######################################

load_dotenv()
marketstack_access_key=os.getenv("MARKETSTACK_ACCESS_KEY")

#today=datetime.today().strftime('%Y-%m-%d')

#def get_marketstack_params(date_from="2020-01-01", date_to=today):
#    return {"date_from":date_from, "date_to": date_to, "sort": "DESC"}


def create_marketstack_url(stock_name='NVDA'):
    return "http://api.marketstack.com/v1/eod?access_key={}&symbols={}".format(marketstack_access_key,stock_name)


def connect_to_marketstack_endpoint(url):
    response = requests.request("GET", url)
    print('code',response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()

url = create_nyt_url('nvidia',10)
json_response = connect_to_nyt_endpoint(url)

stock_data = json.dumps(json_response, indent=4, sort_keys=True)

with open("stock.json", "w") as outfile:
    outfile.write(stock_data)


#save_file=open("stock.json", "w")
#json.dumps(json_response, save_file, indent=6)
#save_file.close()



