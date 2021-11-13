#!/home/jake/anaconda3/envs/jake_env/bin/python3

# The purpose of this script is to call a list of subreddits,
#  pandas-ify the responses, and combine all the data into a
#  single df

import reqs
import pandas as pd
from pull_data import pd_data
import sqlalchemy


# Credit for this connection method:
# https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf
conn_string = 'postgresql://python:python@localhost:5432/postgres'
engine      = sqlalchemy.create_engine(conn_string)


# List of the columns to keep
# psycopg2 cannot handle dictionaries apparently
col_list = ['approved_at_utc', 'subreddit', 'selftext', 'author_fullname', 'saved', 'title', 'downs', 'name', 'upvote_ratio', 'ups', 'total_awards_received', 'category', 'score', 'approved_by', 'created', 'likes', 'author', 'discussion_type', 'num_comments', 'send_replies', 'whitelist_status', 'contest_mode', 'mod_reports', 'author_patreon_flair', 'author_flair_text_color', 'permalink', 'parent_whitelist_status', 'stickied', 'url', 'num_crossposts']


# Pull in the subreddits list
subreddits_list = []

with open("subreddits.txt","r") as file:
    for line in file:
        subreddits_list.append(line.replace('\n',''))


# Make the calls, then add the result to the dataframe
tot_df = pd.DataFrame()

for sub in subreddits_list:
    resp   = reqs.get_posts(sub).json()  # Extract
    df     = pd_data(resp)               # Transform
    tot_df = tot_df.append(df, ignore_index=True)

# Filter out unused columns and load to PostGreSQL table
tot_df = tot_df[col_list]
tot_df.to_sql('test',engine, index=False) #Load
