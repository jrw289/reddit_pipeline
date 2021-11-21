#!/home/jake/anaconda3/envs/jake_env/bin/python3

# LOAD
# The purpose of this script is to read landed JSON files,
#  insert them into a common pandas DataFrame, then perform
#  operations using that DataFrame to load the data into 
#  a temporary database and finally a stable database
import json
import pandas as pd
import sqlalchemy
from os import listdir



# Function to pandas-ify the read Reddit data appropriately
# Must pass valid JSON
def pd_data( json_data):

    # Getting the data into a useful df
    bd_temp_df = pd.DataFrame(json_data['data']['children'])
    bd_df = pd.json_normalize(bd_temp_df['data'], max_level=0)

    return bd_df


def load():

    # CONFIG VALUES
    # In the future, these can be 
    #   specified by a config file on disk
    conn_string   = 'postgresql://python:python@localhost:5432/postgres'
    workdir       = "/home/jake/python_workdir/apis/reddit/workdir/"
    temp_table_name  = 'temp'
    final_table_name = 'test'


    # List of the columns to keep from reddit data 
    # psycopg2 cannot handle dictionaries apparently,
    col_list = ['subreddit', 'selftext', 'author_fullname', 'title', 'name', 'upvote_ratio', 'ups', 'score', 'author', 'num_comments', 'permalink', 'url']

    # Initialize the DataFrame
    tot_df = pd.DataFrame()


    # Find the paths for all the JSON files in workdir
    json_files = [ (workdir + x) for x in listdir(workdir) if ".json" in x]
    
    # Read all the files into the DataFrame 
    for j_file in json_files:
        with open(j_file,'r') as resp:
            j_resp = json.loads(resp.read())
            df     = pd_data(j_resp)               
            tot_df = tot_df.append(df, ignore_index=True)

    # Filter out unused columns and load to PostGreSQL table
    tot_df = tot_df[col_list]


    # Method Source: https://stackoverflow.com/questions/42461959/how-do-i-perform-an-update-of-existing-rows-of-a-db-table-using-a-pandas-datafra
    # Want to perform an UPSERT with our SQL query
    # Using these tools, the method I use is the following:
    #  1) Make a table "temp" and add the latest values to it 
    #  2) Use sqlalchemy's engine to delete all rows matching in temp_table
    #       on some set of columns (currently, 'title' and 'author_fullname')
    #  3) Use sqlalchemy's engine again to insert all the temporary table 
    #       rows into the final table
    # 


    # Connect to the database
    # Credit for this connection method:
    # https://towardsdatascience.com/upload-your-pandas-dataframe-to-your-database-10x-faster-eb6dc6609ddf
    engine = sqlalchemy.create_engine(conn_string)
    tot_df.to_sql(temp_table_name, engine, index=False, if_exists='replace') #1)
    connection = engine.connect()
    transaction = connection.begin()

    print("Load: Starting Upsert process...")
    try:
        # Delete all existing rows in the final table that
        #   match on 'title' and 'author_fullname'
        delete_str = f'delete from {final_table_name} where ({final_table_name}.title, {final_table_name}.author_fullname) in (select A.title, A.author_fullname from {final_table_name} as A inner join {temp_table_name} as B on A.title = B.title and A.author_fullname = B.author_fullname)'
        #print(delete_str)
        engine.execute(delete_str) # 2)
        engine.execute(f'INSERT INTO {final_table_name} (SELECT * from {temp_table_name})') # 3)
        transaction.commit()
        print("Load: Upsert successful")

        engine.execute(f'DROP TABLE {temp_table_name}')
        print(f'Load: Dropped {temp_table_name}')

    except:
        transaction.rollback()
        print("Load Error: Unable to successfully Upsert")
        raise 

