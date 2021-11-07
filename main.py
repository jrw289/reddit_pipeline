#!/home/jake/anaconda3/envs/jake_env/bin/python3

# The purpose of this script is to call a list of subreddits,
#  pandas-ify the responses, and combine all the data into a
#  single df

import reddit_requests
import pandas as pd
from pull_data import pd_data

# Pull in the subreddits list
subreddits_list = []

with open("subreddits.txt","r") as file:
    for line in file:
        subreddits_list.append(line.replace('\n',''))


# Make the calls, then add the result to the dataframe
tot_df = pd.DataFrame()

for sub in subreddits_list:
    resp    = reddit_requests.get_posts(sub).json()
    df     = pd_data(resp)
    tot_df = tot_df.append(df, ignore_index=True)

print(tot_df)
print(tot_df.columns)
