#!/home/jake/anaconda3/envs/jake_env/bin/python3

import reqs
import pandas as pd


# Function to pandas-ify the returned Reddit data 
# Must pass valid JSON
def pd_data( json_data):

    # Getting the data into a useful df
    bd_temp_df = pd.DataFrame(json_data['data']['children'])
    bd_df = pd.json_normalize(bd_temp_df['data'], max_level=0)

    return bd_df


if __name__ == "__main__":
    
    # Make the call
    bd_resp = reqs.get_posts('bigdata').json()
    print( pd_data(bd_resp) )
