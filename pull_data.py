#!/home/jake/anaconda3/envs/jake_env/bin/python3

import reddit_requests
import pandas as pd

# Make the call
bd_resp = reddit_requests.get_posts('bigdata')

# Getting the data into a useful df
bd_temp_df = pd.DataFrame(bd_resp.json()['data']['children'])
bd_df = pd.json_normalize(bd_temp_df['data'])

print(bd_df)
