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


def load(conn_string, workdir, temp_table_name, final_table_name):

    # CONFIG VALUES
    # In the future, these can be 
    #   specified by a config file on disk
    #conn_string   = 'postgresql://python:python@localhost:5432/postgres'
    #workdir       = "/home/jake/python_workdir/apis/reddit/src/workdir/"
    #temp_table_name  = 'temp'
    #final_table_name = 'test'


    # List of the columns to keep from reddit data 
    # psycopg2 cannot handle dictionaries apparently,
    col_list = ['subreddit', 'selftext', 'author_fullname', 'title', 'name', 'upvote_ratio', 'ups', 'score', 'author', 'num_comments', 'permalink', 'url']

    # Initialize Df and engine
    tot_df = pd.DataFrame()
    engine = sqlalchemy.create_engine(conn_string)

    # Find the paths for all the JSON files in workdir
    json_files = [ (workdir + x) for x in listdir(workdir) if ".json" in x]
    
    # Sort on timestamps 
    json_files = sorted(json_files, key=lambda x: x[x.index('_')+1:])
    
    num_json_files = len(json_files)
    print(f"Number of JSON files: {num_json_files}")
    
    # Read files one-by-one into the desired table 
    for count, j_file in enumerate(json_files):
        print(f"Opening file {count} of {num_json_files}")
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

            tot_df.to_sql(temp_table_name, engine, index=False, if_exists='replace') #1)
            connection = engine.connect()
            transaction = connection.begin()

            print("Load: Starting Upsert process...")
            try:
                # Update test rows if changed
                update_str = f"Update {final_table_name} as A SET selftext = temp.selftext, upvote_ratio = temp.upvote_ratio, ups = temp.ups, score = temp.score, num_comments = temp.num_comments, url = temp.url FROM {temp_table_name} as temp WHERE temp.title = A.title and temp.author_fullname = A.author_fullname"
                engine.execute(update_str)
        
        
                # Delete all existing rows in the temp table that
                #   match on 'title' and 'author_fullname'
                delete_str = f'delete from {final_table_name} where ({final_table_name}.title, {final_table_name}.author_fullname) in (select A.title, A.author_fullname from {final_table_name} as A inner join {temp_table_name} as B on A.title = B.title and A.author_fullname = B.author_fullname)'
                #print(delete_str)
                engine.execute(delete_str) # 2)
        
                # Insert all remaining rows into the final table 
                engine.execute(f'INSERT INTO {final_table_name} (SELECT * from {temp_table_name})') # 3)
        
                # Delete all the values in the temp table 
                print("Should be about to delete table values...")
                engine.execute(f'DELETE FROM {temp_table_name}')

                # Commit the transaction 
                transaction.commit()
                print("Load: Upsert successful")


            except:
                transaction.rollback()
                print("Load Error: Unable to successfully Upsert")
                raise 
