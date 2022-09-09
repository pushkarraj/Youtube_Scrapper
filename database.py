import logging

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import gridfs
import pymongo
import requests
import os



def snowflakes(dataframe,table_name,schema):
    ctx = snowflake.connector.connect(
    user='pushkar',
    password='!Push_94302!',
    account='qf98265.ap-southeast-1'
    )
    cs = ctx.cursor()
    try:
        cs.execute("CREATE DATABASE IF NOT EXISTS YOUTUBE1")
        cs.execute("CREATE SCHEMA IF NOT EXISTS YOUTUBE1."+f'"{schema}"')
        cs.execute("USE SCHEMA YOUTUBE1."+f'"{schema}"')
        cs.execute("TRUNCATE TABLE IF EXISTS "+f'"{table_name}"')
        cs.execute("USE SCHEMA YOUTUBE1."+f'"{schema}"')
        write_pandas(ctx,dataframe,table_name,schema=schema,auto_create_table=True)
        one_row = cs.fetchone()
        print(one_row[0])
    except Exception as e:
        logging.error(str(e))
        logging.info("Snowflake DB update failed")
    finally:
        cs.close()
    ctx.close()

def mongo_db_datawriter(db_name, coll_name, dataframe):
    username="youtube_scrapper"
    password="!Push_94302!"

    try:
        CONNECTION_URL = f"mongodb+srv://{username}:{password}@cluster0.j6ufges.mongodb.net/?retryWrites=true&w=majority"
        client = pymongo.MongoClient(CONNECTION_URL)

        dataBase = client[db_name]
        collection = dataBase[coll_name]
        collection.delete_many({})
        collection.insert_many(dataframe.to_dict('records'))
        # all_record = collection.find()
        # for idx, record in enumerate(all_record):
        #     print(f"{idx}: {record}")
    except Exception as e:
        logging.error(e)
        logging.info("Mongodb data writer failed to upload")

def mongo_db_image_writer(db, name, tn):
    username = "youtube_scrapper"
    password = "!Push_94302!"

    CONNECTION_URL = f"mongodb+srv://{username}:{password}@cluster0.j6ufges.mongodb.net/?retryWrites=true&w=majority"
    client = pymongo.MongoClient(CONNECTION_URL)

    database = client[db]
    collection = database[name + ".chunks"]
    collection.delete_many({})
    collection = database[name + ".files"]
    collection.delete_many({})

    fs = gridfs.GridFS(database, name)

    target_folder = os.path.join('./images', '_'.join(name.lower().split(' ')))

    if not os.path.exists(target_folder):
        os.makedirs(target_folder)

    for i in range(0, len(tn)):
        res = requests.get(tn[i])

        with open(os.path.join(target_folder, name + "_" + str(i) + ".jpg"), 'wb') as f:
            f.write(res.content)

        fs.put(res.content, filename=name + "_" + str(i))

# df=pd.read_csv("Comment_Detail.csv",index_col="Unnamed: 0")
# df1=pd.read_csv("Video_Detail.csv",index_col="Unnamed: 0")
# snowflakes(df,"Comment_Detail")
# snowflakes(df1,"Video_Detail")


