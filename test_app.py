# ====================== LIBRARY SETUP ====================== #
# API READER SETUP
from googleapiclient.discovery import build #GOOGLE API
from pandas import json_normalize
# YAML READER SETUP
import os
# PICKLE SETUP
import pickle
# LOG SETUP
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date
import calendar
# PSYCOG2
import psycopg2

def read_pickle(file_name: str) -> pd.DataFrame:
    return pd.read_pickle('Pickle/' + file_name)

def picke_replace(name, file):
    try:
        if not os.path.exists('Pickle/'):
            try:
                os.makedirs('Pickle')
            except FileExistsError:
                pass
    except Exception as e:
        print('Failed to create directory (Pickle/..) ' + name.upper() + e.__str__())
    else:
        print('Successfully created directory (Pickle/..) ' + name.upper())
    # Create pickle file
    try:
        if os.path.exists('Pickle/' + name + '.pkl'):
            with open('Pickle/' + name + '.pkl', 'wb') as f:
                pickle.dump(file, f)
        else:
            file.to_pickle('Pickle/' + name + '.pkl')
    except Exception as e:
        print('Failed to export(.pkl) ' + name.upper() + e.__str__())
    else:
        print('Successfully export(.pkl) ' + name.upper())

def api_youtube_popular(name, max_result):

    # ====================== Setup ====================== #
    pd.options.mode.chained_assignment = None  # Off warning messages, default='warn'
    starttime = datetime.now()
    print(starttime)
    dictionary = {0: 'wiki_category_1', 1: 'wiki_category_2', 2: 'wiki_category_3', 3: 'wiki_category_4',
                  4: 'wiki_category_5', 5: 'wiki_category_6'}
    dictionary_list = list(dictionary.values())
    pickle_name = name

    # ====================== Retrieving API and store in DF  ======================#
    service_key = os.environ['YOUTUBE_API_KEY']
    youtube = build('youtube', 'v3', developerKey=service_key)

    try:
        # YOUTUBE_VIDEO_LIST
        res_popular = youtube.videos().list(part=['snippet', 'statistics', 'status', 'topicDetails'],
                                            chart='mostPopular',
                                            maxResults=int(max_result), regionCode='KR').execute()
        df_popular = json_normalize(res_popular['items'])
        print('Videos API Connection has been successfully completed')

        # YOUTUBE_VIDEO_CATEGORY
        res_videocategory = youtube.videoCategories().list(part='snippet', regionCode='KR').execute()
        df_videocategory = json_normalize(res_videocategory['items'])
        df_videocategory = df_videocategory[['id', 'snippet.title']]
        print('VideoCategories API Connection has been successfully completed')
    except:
        print(name.upper() + ' has failed to open API Connection')

    # ====================== YOUTUBE_VIDEO_LIST : Data Mapping  ======================#

    # Select Columns
    df_popular = df_popular[
        ['snippet.title', 'id', 'snippet.channelTitle', 'snippet.channelId', 'snippet.publishedAt', 'snippet.tags',
         'snippet.categoryId',  # video().list(part='snippet')
         'statistics.viewCount', 'statistics.likeCount', 'statistics.dislikeCount', 'statistics.favoriteCount',
         'statistics.commentCount',  # video().list(part='statistics')
         'topicDetails.topicCategories',  # video().list(part='topicDetails')
         'status.madeForKids']]

    # Rename Columns
    df_popular.rename(columns={'snippet.title': 'video_title',
                               'id': 'video_id',
                               'snippet.channelTitle': 'channel_title',
                               'snippet.channelId': 'channel_id',
                               'snippet.publishedAt': 'published_at',
                               'snippet.tags': 'tags',
                               'snippet.categoryId': 'category_id',
                               'statistics.viewCount': 'view_count',
                               'statistics.likeCount': 'like_count',
                               'statistics.dislikeCount': 'dislike_count',
                               'statistics.favoriteCount': 'favorite_count',
                               'statistics.commentCount': 'comment_count',
                               'topicDetails.topicCategories': 'topic_categories',
                               'status.madeForKids': 'for_kids',
                               }, inplace=True)

    # Reset Index
    df_popular = df_popular.reset_index(drop=True)

    # Split TopicCategories URL
    catrgory_split = df_popular['topic_categories']
    catrgory_split = pd.DataFrame(catrgory_split)
    catrgory_split = catrgory_split['topic_categories'].apply(pd.Series).rename(columns=dictionary)

    # Filter columns based on the length
    dictionary_list = dictionary_list[0:len(catrgory_split.columns)]

    # Split WIKI_URL and pick up the last word (Filtering category)
    for i in range(len(catrgory_split.columns)):
        df = catrgory_split.iloc[:, i].str.split('/').apply(pd.Series).iloc[:, -1]
        df.columns = [i]
        catrgory_split[i] = df

    # Remove & Rename columns
    catrgory_split.drop(dictionary_list, axis=1, inplace=True)
    catrgory_split = catrgory_split.rename(columns=dictionary)

    # Merge & Rename columns
    df_popular = df_popular.merge(catrgory_split, left_index=True, right_index=True)
    del df_popular['topic_categories']
    print('Youtube Video List : Data mapping has been successfully completed')

    # ====================== YOUTUBE_VIDEO_CATEGORY : Data Mapping  ======================#
    df_videocategory = df_videocategory[['id', 'snippet.title']]
    df_videocategory.rename(columns={'id': 'category_id',
                                     'snippet.title': 'reg_category'
                                     }, inplace=True)
    print('Youtube Video Category : Data mapping has been successfully completed')

    # ====================== MERGE : df_popular & df_videocategory ====================== #
    df_popular = df_popular.merge(df_videocategory, how='inner', on='category_id')

    # ====================== Export to Pickle & Read  ======================#
    picke_replace(name=pickle_name, file=df_popular)
    youtube_popular = read_pickle('youtube_popular.pkl')
    # ======================================================================#

    endtime = datetime.now()
    print(endtime)
    timetaken = endtime - starttime
    print('Time taken : ' + timetaken.__str__())

    return youtube_popular

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
        print("Connection successful")
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

def config(host, database, user, port, password):

    params_dic = {
        "host": host,
        "database": database,
        "port": port,
        "user":user,
        "password": password
    }
    return params_dic

def insert_table(curr, run_date, day, video_title, video_id, channel_title, channel_id, published_at, tags, category_id, view_count, like_count, dislike_count, favorite_count, comment_count, for_kids, wiki_category_1, wiki_category_2, wiki_category_3, wiki_category_4, reg_category):

    insert_into_videos = "INSERT INTO popular_chart (run_date, day, video_title, video_id, channel_title, channel_id, published_at, tags, category_id, view_count, like_count, dislike_count, favorite_count, comment_count, for_kids, wiki_category_1, wiki_category_2, wiki_category_3, wiki_category_4, reg_category) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    row_to_insert = (run_date, day, video_title, video_id, channel_title, channel_id, published_at, tags, category_id, view_count, like_count, dislike_count, favorite_count, comment_count, for_kids, wiki_category_1, wiki_category_2, wiki_category_3, wiki_category_4, reg_category)
    try:
        curr.execute(insert_into_videos, row_to_insert)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def append_db(curr, conn, df):

    try:
        for i, row in df.iterrows():
            insert_table(curr, row['run_date'], row['day'], row['video_title'], row['video_id'], row['channel_title'],
                         row['channel_id'], row['published_at'], row['tags'], row['category_id'], row['view_count'],
                         row['like_count'], row['dislike_count'], row['favorite_count'], row['comment_count'], row['for_kids'],
                         row['wiki_category_1'], row['wiki_category_2'], row['wiki_category_3'], row['wiki_category_4'], row['reg_category'])
            conn.commit()
        curr.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def postgresql_run():

    starttime = datetime.now()
    print(starttime)
    youtube_popular = api_youtube_popular(name='youtube_popular', max_result=20)
    print(youtube_popular)

    # Move last column(run_date) to first sequence
    youtube_popular['run_date'] = date.today()
    youtube_popular['day'] = calendar.day_name[date.today().weekday()]
    cols = youtube_popular.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    youtube_popular = youtube_popular[cols]

    # Connect to Postgresql
    host = os.environ['SQL_HOST']
    database = os.environ['SQL_DB']
    user = os.environ['SQL_USER']
    port = os.environ['SQL_PORT']
    password = os.environ['SQL_PW']

    params_dic = config(host=host, database=database, user=user, port=port, password=password)
    conn = connect(params_dic, sslmode='require')
    curr = conn.cursor()

    # Inserting each row
    append_db(curr=curr, conn=conn, df=youtube_popular)

    endtime = datetime.now()
    print(endtime)
    timetaken = endtime - starttime
    print('Time taken : ' + timetaken.__str__())

postgresql_run()