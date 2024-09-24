import os
from dotenv import load_dotenv

from datetime import datetime
import requests
import json
import pandas as pd


# load API key
load_dotenv()
nyt_key=os.getenv("NYT_KEY")


def create_nyt_url(stock_name,page):
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
    return response.json() 

######## Fetch all the content, if the page contains 10 articles, then we continue the next page   ############


url = create_nyt_url('nvidia',10)
json_response = connect_to_nyt_endpoint(url)

nyt_data = json.dumps(json_response, indent=4, sort_keys=True)
nyt_data = json.loads(nyt_data)
print(len(nyt_data['response']['docs']))

temp_abstract=[]
temp_pub_date=[]
temp_document_type=[]
for _ in range(len(nyt_data['response']['docs'])):
    temp_abstract.append(nyt_data['response']['docs'][_]['abstract'])
    temp_pub_date.append(nyt_data['response']['docs'][_]['pub_date'])
    temp_document_type.append(nyt_data['response']['docs'][_]['document_type'])

dict = {'abstract': temp_abstract, 'pub_date': temp_pub_date, 'document_type':temp_document_type }

df = pd.DataFrame(dict)


    


#print(nyt_data) #stock price is what we need
#nyt_data.values()




