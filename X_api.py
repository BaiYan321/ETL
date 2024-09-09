# pip install git+https://github.com/tweepy/tweepy.git

import os
from dotenv import load_dotenv

from requests_oauthlib import OAuth1Session
import requests
import json

# load API key
load_dotenv()
api_key = os.getenv('API_KEY')
api_secret_key = os.getenv('API_SECRET_KEY')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
bearer_token = os.getenv("BEARER_TOKEN")

# User fields are adjustable, options include:
# created_at, description, entities, id, location, name,
# pinned_tweet_id, profile_image_url, protected,
# public_metrics, url, username, verified, and withheld

def get_params():
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    return {"tweet.fields": "created_at"}
    #return {"expansions":"pinned_tweet_id"}

def create_url(user_name='elonmask'):
    # Replace with user ID below
    #user_id = 2244994945
    #return "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    #params=get_params()
    #return "https://api.twitter.com/2/users/by/username/{}?expansions=pinned_tweet_id&tweet.fields=created_at".format(user_name)
    return "https://api.twitter.com/2/users/by/username/{}?expansions=pinned_tweet_id".format(user_name)

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r

def connect_to_endpoint(url):
    response = requests.request("GET", url, auth=bearer_oauth)
    print('code',response.status_code)
    print('response',response)

    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def main():
    url = create_url()
    json_response = connect_to_endpoint(url)
    print('json_response', json_response)
    print(json.dumps(json_response, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
