#!/home/jake/anaconda3/envs/jake_env/bin/python3

import sys
import requests 
import json


# Make sure the token is still valid or the response will be in HTML

# Adding the access_token to the header
# NOTE: Below is bad form, but was temporarily done for simplicity
#token_file = open("at.txt","r")

def make_headers(token_path):
    try:
        token_file = open(token_path,"r")
    except:
        raise

    auth_head = "Bearer " + token_file.readline().replace('\n','')

    headers = {"Authorization": auth_head,
               "User-Agent": "And I, for one, welcome our new insect overlords"}

    token_file.close()

    return headers


def get_posts(url, token_path):
    
    resp = requests.get("https://oauth.reddit.com/r/" + url, headers=make_headers(token_path))

    return resp


