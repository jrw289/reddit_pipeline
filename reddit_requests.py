#!/home/jake/anaconda3/envs/jake_env/bin/python3

import sys
import requests 
import json


# Make sure the token is still valid or the response will be in HTML

# Adding the access_token to the header
token_file = open("at.txt","r")
auth_head = "Bearer " + token_file.readline().replace('\n','')

headers = {"Authorization": auth_head,
           "User-Agent": "Fuck Reddit devs"}

token_file.close()


def get_posts(url):
    
    resp = requests.get("https://oauth.reddit.com/r/" + url, headers=headers)

    return resp


if __name__ == "__main__":

    # Asking the user for the desired subreddit
    tot_url = input("Put in a subreddit, starting after r/: ")
    save_to_file = input("\nWould you like to save this result to file?: ")
    if save_to_file.lower() not in ['y', 'n', 'yes', 'no']:
        print("Input not understood, cancelling operation")
        sys.exit()

    output = get_posts(tot_url).json()

    if save_to_file.lower() in ['y', 'yes']:
        with open("data.txt","w") as file:
            file.write(json.dumps(output, indent=1))
            file.close()

    else:
    # output is already in JSON format, but this output has cleaner indenting
        print(json.dumps(output, indent=1))
