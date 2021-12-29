#!/usr/bin/python3

# EXTRACT 
# The purpose of this script is to call a list of subreddits,
#  convert that data to json, then save the data to a JSON file

import extract.reqs as reqs
import json
from datetime import datetime



# Opens file with list of desired subreddits, then constructs
#  a list object out of the subreddits
def get_subreddits_list(file_path):

    temp_list = []
    
    # Read the subreddits list off disk
    try:
        with open(file_path,"r") as file:
            for line in file:
                temp_list.append(line.replace('\n',''))
                        
        return temp_list
    
    except:
        print('Extract Error: Problem with {} processing'.format(file_path))
        raise


# Makes a requests call to the subreddit and converts the
#  response JSON into a string that is human-readable
def request_subbreddit_data(subreddit,token_path):
    try:
        request  = reqs.get_posts(subreddit, token_path)
        resp     = request.json()
        output   = json.dumps(resp, indent=1)
        return output
    
    except:
        print('Extract Error: Problem with request or converting response to JSON')
        print('Extract Error:   Response Code - {}'.format(request.status_code))
        print('Extract Error:   Reason        - {}'.format(request.reason))
            
# Saves the JSON string into a file on disk 
def save_json_file(file_name, data):
    try:
        with open(file_name,'w') as t:
            t.write(data)
    except:
        print("Extract Error: Unable to open file {}".format(file_name))



# Main function called in Airflow 
def extract(sr_path, workdir, token_path):
    
    #sr_path = "/home/jake/pydir/apis/reddit/subreddits.txt"
    #workdir = "/home/jake/pydir/apis/reddit/workdir/"
    
    # Pull the subreddits to call
    subreddits_list = get_subreddits_list(sr_path)
    print('Extract retrieved subreddits list successfully')
    
    # Save each request as a landed JSON file 
    for sub in subreddits_list:
        
        sub_data = request_subbreddit_data(sub, token_path)
        
        temp_name = workdir + sub + "_" \
                    + datetime.now().strftime('%Y-%m-%d_%H%M%S') + \
                    '.json'
                    
        save_json_file(temp_name, sub_data)
        print('Extract for subreddit {} successful'.format(sub))
        
    print('Extract successfully completed')
