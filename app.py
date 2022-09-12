from googleapiclient.discovery import build
import pandas as pd
from flask import Flask , render_template , request , jsonify
from bs4 import BeautifulSoup as y_bs
from urllib.request import urlopen as y_urReq
import json
from flask_cors import CORS, cross_origin
import pymongo
from sqlalchemy import create_engine
import mysql.connector as conn
import sqlalchemy as db
from csvkit.utilities.csvsql import CSVSQL as csvkit_sql

app = Flask(__name__)


@app.route('/', methods=['GET'])  # route to display the home page
@cross_origin()
def homePage():
    return render_template("index.html")


@app.route('/review', methods=['POST', 'GET'])  # route to show the review comments in a web UI
@cross_origin()
def index():
    if request.method == 'POST':
        try:
            # Get channel id(s) for input channel(s)
            input_channels = "Telusko,Krish Naik,HiteshChoudharydotcom"
            # input_channels is to take multiple channels' sep by coma
            yt_channels = request.form['content'].split(',')
            # if usernames or channel names are given as input then below function is to find channel ids.
            yt_chnl_id, yt_chnl_url = chnl_id_extract(yt_channels)

            # API key YouTube service build. To extract the channel analytics with YouTube api need api key to access
            api_key = 'AIzaSyB37mzehupaiz8QjN8iVkUEVBLhe5cfTdk'
            channel_ids = yt_chnl_id
            youtube = build('youtube', 'v3', developerKey=api_key)
            channel_statistics, users_playlist_ids = get_channel_stats(youtube, channel_ids)
            channel_data = pd.DataFrame(channel_statistics)
            # yt_chnl_id, yt_chnl_url, channel_statistics
            # for play_list_id in users_playlist_ids:
            maxvideos = 50  # set video extraction limit to 50 as i hit with error IOPub data rate exceeded.
            # The notebook server will temporarily stop sending output to the client in order to avoid crashing it.
            cumm_channel_video_stats = []
            channel_all_videos_cmnts = []
            for i in range(len(users_playlist_ids)):
                video_ids = []
                video_ids_all = get_video_ids(youtube, users_playlist_ids[i])
                video_ids = video_ids_all[0:maxvideos]
                #print(len(video_ids))
                channel_video_stats, channel_videos_cmnts = get_video_details(youtube, video_ids, channel_statistics[i],
                                                                              yt_chnl_id[i], maxvideos)
                cumm_channel_video_stats.extend(channel_video_stats)
                channel_all_videos_cmnts.extend(channel_videos_cmnts)
            # Channel Stats
            channel_video_stats_data = pd.DataFrame(cumm_channel_video_stats)
            channel_video_stats_data_copy = channel_video_stats_data
            channel_video_stats_data['Published_date'] = pd.to_datetime(
                channel_video_stats_data['Published_date']).dt.date
            channel_video_stats_data['Channel_Subscribers'] = pd.to_numeric(
                channel_video_stats_data['Channel_Subscribers'])
            channel_video_stats_data['Channel_Views'] = pd.to_numeric(channel_video_stats_data['Channel_Views'])
            channel_video_stats_data['Total_videos'] = pd.to_numeric(channel_video_stats_data['Total_videos'])
            channel_video_stats_data['Views'] = pd.to_numeric(channel_video_stats_data['Views'])
            channel_video_stats_data['Likes'] = pd.to_numeric(channel_video_stats_data['Likes'])
            channel_video_stats_data['Comments'] = pd.to_numeric(channel_video_stats_data['Comments'])

            channel_video_stats_data.to_csv('Channel_Video_Stats.csv')

            try:
                ats_engine = create_engine('mysql+pymysql://root:root1234@localhost/Inventory')
                result = ats_engine.execute("drop table Channel_Video_Stats;")
            except:
                pass
            try:
                '!csvsql - -db mysql + pymysql: // root: root1234 @ localhost:3306 / Inventory - -tables Channel_Video_Stats - -insert - -create - if -not -exists Channel_Video_Stats.csv'
            except:
                pass
            #print('sql is done')
            # Channel Comment Stats
            channel_all_videos_cmnts_lst = []
            for i in range(len(channel_all_videos_cmnts)):
                for j in range(len(channel_all_videos_cmnts[i])):
                    channel_all_videos_cmnts_lst.append(channel_all_videos_cmnts[i][j])
                    # print(channel_all_videos_cmnts[i][j])
            len(channel_all_videos_cmnts_lst)
            channel_all_videos_cmnts_df = pd.DataFrame(channel_all_videos_cmnts_lst)
            channel_all_videos_cmnts_df_copy = channel_all_videos_cmnts_df
            channel_all_videos_cmnts_df_copy = channel_all_videos_cmnts_df
            channel_all_videos_cmnts_df['ChannelComPubdt'] = pd.to_datetime(
                channel_all_videos_cmnts_df['ChannelComPubdt'])
            channel_all_videos_cmnts_df['VideoUserTxtPubDt'] = pd.to_datetime(
                channel_all_videos_cmnts_df['VideoUserTxtPubDt'])
            channel_all_videos_cmnts_df['ChannelCommentLikes'] = pd.to_numeric(
                channel_all_videos_cmnts_df['ChannelCommentLikes'])
            channel_all_videos_cmnts_df['ChanneltotalReplyCount'] = pd.to_numeric(
                channel_all_videos_cmnts_df['ChanneltotalReplyCount'])
            channel_all_videos_cmnts_df['VideoUserTxtLikCnt'] = pd.to_numeric(
                channel_all_videos_cmnts_df['VideoUserTxtLikCnt'])
            channel_all_videos_cmnts_df['VideoUserTxtTRCnt'] = pd.to_numeric(
                channel_all_videos_cmnts_df['VideoUserTxtTRCnt'])
            #channel_all_videos_cmnts_df.head()
            #print('comments df is done')
            channel_all_videos_cmnts_df.to_csv('Channel_Video_Comments.csv')
            try:

                client = pymongo.MongoClient(
                    "mongodb+srv://mongodb:mongoroot@cluster0.ko8ye.mongodb.net/?retryWrites=true&w=majority")
                database = client['Analytics']
                database.collection.insert_many(channel_all_videos_cmnts_df.to_dict('records'))
            except:
                pass
            #print('mongo db comments df is done')
            return render_template('results.html', reviews=cumm_channel_video_stats[0:(len(cumm_channel_video_stats) - 1)])
        except Exception as e:
            #print('The Exception message is: ', e)
            return 'something is wrong'
    # return render_template('results.html')

    else:
        return render_template('index.html')


# Extracting channel ids for better analytics of channel
def chnl_id_extract(channels):
    channel_id = []
    channel_url = []
    for channel in channels:
        try:
            channel = channel.replace(" ", "+")
            search_url = "https://www.youtube.com/results?search_query="
            youtube_channel = search_url + channel
            #print(youtube_channel)
            response_website = y_urReq(youtube_channel)
            data_flipcart = response_website.read()
            beautifyed_html = y_bs(data_flipcart, "html.parser")
            bigbox = beautifyed_html.select_one("script:contains('{\"contents\":[{\"itemSectionRenderer\":')")
            yt_chnl_script_list_content_list = bigbox.contents[0][20:-1]
            yt_chnl_script_list_content_list_json = json.loads(yt_chnl_script_list_content_list)
            yt_chnl_script_json_tabs = \
            yt_chnl_script_list_content_list_json['contents']['twoColumnSearchResultsRenderer']['primaryContents']
            try:
                yt_chnl_info = \
                yt_chnl_script_json_tabs['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][0][
                    'channelRenderer']
            except:
                yt_chnl_info1 = \
                yt_chnl_script_json_tabs['sectionListRenderer']['contents'][0]['itemSectionRenderer']['contents'][1]
                yt_chnl_info = yt_chnl_info1['videoRenderer']['longBylineText']['runs'][
                    0]  # ['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']

            channel_url.append(yt_chnl_info['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url'])
            try:
                channel_id.append(yt_chnl_info['channelId'])
            except:
                channel_id.append(yt_chnl_info['navigationEndpoint']['browseEndpoint']['browseId'])

        except:
            pass
    #print(channel_id, channel_url)
    return channel_id, channel_url


def get_channel_stats(youtube, channel_ids):
    all_data = []
    playlist = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=','.join(channel_ids))
    response = request.execute()

    for i in range(len(response['items'])):
        # print(response)
        data = dict(Channel_name=response['items'][i]['snippet']['title'],
                    Subscribers=response['items'][i]['statistics']['subscriberCount'],
                    Views=response['items'][i]['statistics']['viewCount'],
                    Total_videos=response['items'][i]['statistics']['videoCount'],
                    playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
        playlist.append(response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])

    return all_data, playlist


def get_video_ids(youtube, playlist_id):
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50)
    response = request.execute()

    video_ids = []

    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])

    next_page_token = response.get('nextPageToken')  # to aviod crash of notebook for huge data
    more_pages = True

    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            #    next_page_token = None
            next_page_token = response.get('nextPageToken')

    return video_ids


def get_video_details(youtube, video_ids, channel_statistics, yt_chnl_id, maxvideos):
    all_video_stats = []
    all_videos_cmnts = []

    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list(
            part='snippet,statistics',
            id=','.join(video_ids[i:i + 50]))
        response = request.execute()

        for video in response['items']:
            # print(response['items'])
            video_stats = dict(ChannelName=video['snippet']['channelTitle'],
                               Channel_Subscribers=channel_statistics['Subscribers'],
                               Channel_Views=channel_statistics['Views'],
                               Total_videos=channel_statistics['Total_videos'],
                               ChannelId=video['snippet']['channelId'],
                               Video_Title=video['snippet']['title'],
                               Video_id=video['id'],
                               Published_date=video['snippet']['publishedAt'],
                               Views=video['statistics']['viewCount'],
                               Likes=video['statistics']['likeCount'],
                               Comments=video['statistics']['commentCount'],
                               Thumbnails=video['snippet']['thumbnails']['high']['url'],
                               VideoLink='https://www.youtube.com/watch?v=' + video['id'],
                               Channel_URL='https://www.youtube.com/channel/' + yt_chnl_id
                               )
            all_video_stats.append(video_stats)
            maxcomments = 10
            video_comments = []
            channel_url = ""
            channel_url = 'https://www.youtube.com/channel/' + yt_chnl_id
            video_comments = get_video_comment_details(youtube, video['id'], video['snippet']['title'], maxcomments,
                                                       video['snippet']['channelTitle'], channel_url)
            all_videos_cmnts.append(video_comments)
    return all_video_stats, all_videos_cmnts


def get_video_comment_details(youtube, video_id, Video_Title, maxcomments, chnlowner, channel_url):
    all_video_comments = []
    # print(video_id,Video_Title,maxcomments)
    # for k in range(0, len(video_ids), 50):
    request = youtube.commentThreads().list(
        part='snippet,replies',
        videoId=video_id,
        #           id=','.join(video_id[k:k+50]),
        maxResults=10)
    response = request.execute()
    # print(response)

    for i in range(len(response['items'])):
        # print(response['items'])
        # print("inresponse")
        if i == 0:
            ChannelOwner_0 = chnlowner
            ChannelName_0 = channel_url
            VideoID_0 = response['items'][i]['snippet']['videoId']
            VideoTitle_0 = Video_Title
            ChannelCommenter_0 = chnlowner  # commenter name
            ChannelComment_0 = response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay']  # comment
            ChannelPic_0 = response['items'][i]['snippet']['topLevelComment']['snippet']['authorProfileImageUrl']  # pic
            ChannelCommentLikes_0 = response['items'][i]['snippet']['topLevelComment']['snippet'][
                'likeCount']  # likeCount
            ChanneltotalReplyCount_0 = response['items'][i]['snippet']['totalReplyCount']  # totalReplyCount
            ChannelComPubdt_0 = response['items'][i]['snippet']['topLevelComment']['snippet'][
                'publishedAt']  # published
            # print(response['items'][i]['replies'])
            try:
                for j in range(len(response['items'][i]['replies']['comments'])):
                    video_comments = dict(
                        ChannelOwner=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        ChannelName=response['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelUrl'],
                        VideoID=response['items'][i]['snippet']['videoId'],
                        VideoTitle=Video_Title,
                        ChannelCommenter=response['items'][i]['snippet']['topLevelComment']['snippet'][
                            'authorDisplayName'],  # commenter name
                        ChannelComment=response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                        # comment
                        ChannelPic=response['items'][i]['snippet']['topLevelComment']['snippet'][
                            'authorProfileImageUrl'],  # pic
                        ChannelCommentLikes=response['items'][i]['snippet']['topLevelComment']['snippet']['likeCount'],
                        # likeCount
                        ChanneltotalReplyCount=response['items'][i]['snippet']['totalReplyCount'],  # totalReplyCount
                        ChannelComPubdt=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                        # published
                        VideoUser=response['items'][i]['replies']['comments'][j]['snippet']['authorDisplayName'],
                        VideoUserChnl=response['items'][i]['replies']['comments'][j]['snippet']['authorChannelUrl'],
                        VideoUserImg=response['items'][i]['replies']['comments'][j]['snippet']['authorProfileImageUrl'],
                        VideoUserTxt=response['items'][i]['replies']['comments'][j]['snippet']['textDisplay'],
                        VideoUserTxtLikCnt=response['items'][i]['replies']['comments'][j]['snippet']['likeCount'],
                        VideoUserTxtTRCnt=0,
                        VideoUserTxtPubDt=response['items'][i]['replies']['comments'][j]['snippet']['publishedAt'])
            except:

                video_comments = dict(ChannelOwner=ChannelOwner_0,
                                      ChannelName=ChannelName_0,
                                      VideoID=response['items'][i]['snippet']['videoId'],
                                      VideoTitle=Video_Title,
                                      ChannelCommenter=ChannelCommenter_0,  # commenter name
                                      ChannelComment=ChannelComment_0,  # comment
                                      ChannelPic=ChannelPic_0,  # pic
                                      ChannelCommentLikes=ChannelCommentLikes_0,  # likeCount
                                      ChanneltotalReplyCount=ChanneltotalReplyCount_0,  # totalReplyCount
                                      ChannelComPubdt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'publishedAt'],  # publi
                                      VideoUser=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'authorDisplayName'],
                                      VideoUserChnl=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'authorChannelUrl'],
                                      VideoUserImg=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'authorProfileImageUrl'],  # pic
                                      VideoUserTxt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'textDisplay'],  # comment
                                      VideoUserTxtLikCnt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'likeCount'],  # likeCount
                                      VideoUserTxtTRCnt=response['items'][i]['snippet']['totalReplyCount'],
                                      # totalReplyCount
                                      VideoUserTxtPubDt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                          'publishedAt'])  # pubdt
                # print(response['items'][i]['replies']['comments'][j]['totalReplyCount'])
        else:
            video_comments = dict(ChannelOwner=ChannelOwner_0,
                                  ChannelName=ChannelName_0,
                                  VideoID=response['items'][i]['snippet']['videoId'],
                                  VideoTitle=Video_Title,
                                  ChannelCommenter=ChannelCommenter_0,  # commenter name
                                  ChannelComment=ChannelComment_0,  # comment
                                  ChannelPic=ChannelPic_0,  # pic
                                  ChannelCommentLikes=ChannelCommentLikes_0,  # likeCount
                                  ChanneltotalReplyCount=ChanneltotalReplyCount_0,  # totalReplyCount
                                  ChannelComPubdt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'publishedAt'],  # publi
                                  VideoUser=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'authorDisplayName'],
                                  VideoUserChnl=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'authorChannelUrl'],
                                  VideoUserImg=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'authorProfileImageUrl'],  # pic
                                  VideoUserTxt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'textDisplay'],  # comment
                                  VideoUserTxtLikCnt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'likeCount'],  # likeCount
                                  VideoUserTxtTRCnt=response['items'][i]['snippet']['totalReplyCount'],
                                  # totalReplyCount
                                  VideoUserTxtPubDt=response['items'][i]['snippet']['topLevelComment']['snippet'][
                                      'publishedAt'])  # pubdt
        # print(video_comments)
        all_video_comments.append(video_comments)

    return all_video_comments


if __name__ == "__main__":
    app.run()
    # app.run(host='127.0.0.1', port=8001, debug=True)
 #   app.run(debug=True)
