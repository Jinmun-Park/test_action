# ====================== LIBRARY SETUP ====================== #
# API READER SETUP
from googleapiclient.discovery import build #GOOGLE API
from pandas import json_normalize
# YAML READER SETUP
import os
from datetime import datetime
# PICKLE SETUP
import pickle
# LOG SETUP
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date
import calendar

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
    dictionary = {0: 'Wiki_Category_1', 1: 'Wiki_Category_2', 2: 'Wiki_Category_3', 3: 'Wiki_Category_4',
                  4: 'Wiki_Category_5', 5: 'Wiki_Category_6'}
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
    df_popular.rename(columns={'snippet.title': 'VideoTitle',
                               'id': 'VideoId',
                               'snippet.channelTitle': 'ChannelTitle',
                               'snippet.channelId': 'ChannelId',
                               'snippet.publishedAt': 'PublishedAt',
                               'snippet.tags': 'Tags',
                               'snippet.categoryId': 'CategoryId',
                               'statistics.viewCount': 'ViewCount',
                               'statistics.likeCount': 'LikeCount',
                               'statistics.dislikeCount': 'DislikeCount',
                               'statistics.favoriteCount': 'FavoriteCount',
                               'topicDetails.topicCategories': 'TopicCategories',
                               'status.madeForKids': 'ForKids',
                               }, inplace=True)

    # Reset Index
    df_popular = df_popular.reset_index(drop=True)

    # Split TopicCategories URL
    catrgory_split = df_popular['TopicCategories']
    catrgory_split = pd.DataFrame(catrgory_split)
    catrgory_split = catrgory_split['TopicCategories'].apply(pd.Series).rename(columns=dictionary)

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
    del df_popular['TopicCategories']
    print('Youtube Video List : Data mapping has been successfully completed')

    # ====================== YOUTUBE_VIDEO_CATEGORY : Data Mapping  ======================#
    df_videocategory = df_videocategory[['id', 'snippet.title']]
    df_videocategory.rename(columns={'id': 'CategoryId',
                                     'snippet.title': 'Reg_Category'
                                     }, inplace=True)
    print('Youtube Video Category : Data mapping has been successfully completed')

    # ====================== MERGE : df_popular & df_videocategory ====================== #
    df_popular = df_popular.merge(df_videocategory, how='inner', on='CategoryId')

    # ====================== Export to Pickle & Read  ======================#
    picke_replace(name=pickle_name, file=df_popular)
    youtube_popular = read_pickle('youtube_popular.pkl')
    # ======================================================================#

    endtime = datetime.now()
    print(endtime)
    timetaken = endtime - starttime
    print('Time taken : ' + timetaken.__str__())

    return youtube_popular
  
def chart_export(key):

    starttime = datetime.now()
    print(starttime)
    youtube_popular = api_youtube_popular(name='youtube_popular', max_result=20)

    # Move last column(run_date) to first sequence
    youtube_popular['run_date'] = date.today()
    youtube_popular['day'] = calendar.day_name[date.today().weekday()]
    cols = youtube_popular.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    youtube_popular = youtube_popular[cols]

    if key == 'new':
        # CASE 1 : Push DF to Table
        params = os.environ['SQL_URL']
        engine = create_engine(params)
        youtube_popular.to_sql('popular_chart', engine, index=False)

    elif key == 'update':
        # CASE 2 : Append DF to Table
        params = os.environ['SQL_URL']
        engine = create_engine(params)
        youtube_popular.to_sql('popular_chart', engine, if_exists='append', index=False)

    endtime = datetime.now()
    print(endtime)
    timetaken = endtime - starttime
    print('Time taken : ' + timetaken.__str__())

def test_app():
    chart_export()

test_app()