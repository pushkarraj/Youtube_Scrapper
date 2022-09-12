from snowflake.connector.pandas_tools import write_pandas
from pytube import YouTube
import snowflake.connector
import requests
import pymongo
import logging
import gridfs
import boto3
import os



def snowflakes(*args):
    ctx = snowflake.connector.connect(
    user='pushkar',
    password='!Push_94302!',
    account='qf98265.ap-southeast-1'
    )
    cs = ctx.cursor()
    try:
        for dataframe,table_name,schema in args:
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

def mongo_db_datawriter(*args):
    username="youtube_scrapper"
    password="!Push_94302!"
    CONNECTION_URL = f"mongodb+srv://{username}:{password}@cluster0.j6ufges.mongodb.net/?retryWrites=true&w=majority"
    client = pymongo.MongoClient(CONNECTION_URL)

    try:
        for db_name, coll_name, dataframe in args:
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
    print("Image Binary uploded to MongoDB")

def youtube_dld_upd(l):
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-1',
        aws_access_key_id='AKIA2MPA4VZQTQM2Q7RU',
        aws_secret_access_key='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')

    for link in l:
        yt = YouTube(link)
        if yt.author not in os.listdir("video"):
            os.mkdir("video" + "/" + str(yt.author))
        ys = yt.streams.get_lowest_resolution()

        print("Downloading...")
        ys.download("video" + "/" + str(yt.author))
        print("Download completed!!"+yt.title)

    if "youtubevideo007" not in [i.name for i in s3.buckets.all()]:
        s3.create_bucket(Bucket="youtubevideo007")
    s3.Bucket("youtubevideo007").objects.all().delete()
    for i in os.listdir("video" + "/" + yt.author):
        s3.Bucket("youtubevideo007").upload_file(os.path.join("video" + "/" + yt.author + "/", i), yt.author + "_"+i)
    print("all videos downloded & uploaded")

# df=pd.read_csv("Comment_Detail.csv",index_col="Unnamed: 0")
# df1=pd.read_csv("Video_Detail.csv",index_col="Unnamed: 0")
# snowflakes(df,"Comment_Detail")
# snowflakes(df1,"Video_Detail")


