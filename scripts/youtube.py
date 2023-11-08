import os
import json
import psycopg2

from googleapiclient.discovery import build
from dotenv import load_dotenv

load_dotenv()


class YouTubeDataExtractor:
        
    @staticmethod
    def get_service():
        service = build("youtube", "v3", developerKey=os.getenv("API_KEY"))
        return service
    
    
    def search_videos(self, queries, min_subscribers=1000, last_dag_run=None):
        args = {
            'type': 'video',
            'relevanceLanguage': 'en',
            'order': 'relevance',
            'maxResults': 5,
            'eventType': 'completed',
            'part': 'snippet',
        }
        youtube = self.get_service()
        video_dict = {}

        if last_dag_run:
            last_dag_run_str = last_dag_run.strftime('%Y-%m-%dT%H:%M:%SZ')
            args['publishedAfter'] = last_dag_run_str

        for query in queries:
            args['q'] = query
            request = youtube.search().list(**args)
            response = request.execute()
            videos = response.get("items", [])
            video_ids = []
        
            for video in videos:
                video_id = video["id"]["videoId"]
                channel_id = video["snippet"]["channelId"]
                video_info = youtube.channels().list(
                    part="snippet,statistics",
                    id=channel_id
                ).execute()
    
                if "items" in video_info and len(video_info["items"]) > 0:
                    subscribers = int(video_info["items"][0]["statistics"]["subscriberCount"])
                    if subscribers >= min_subscribers:
                        video_ids.append(video_id)
            
            video_dict[query] = video_ids
    
        return video_dict
    

    def get_videos_info(self, video_dict):
        video_info_list = []
        channels = set()
        video_list = list(set(v for video_ids in video_dict.values() for v in video_ids))
        for video_id in video_list:
            r = self.get_service().videos().list(id=video_id, part="snippet,statistics,contentDetails").execute()
            for item in r.get("items", []):
                video_id = item["id"]
                snippet = item['snippet']
                conntent_details = item['contentDetails']
                statistics = item['statistics']
                video_info = {
                    'video_id': video_id,
                    'video_title': snippet['title'],
                    'video_description': snippet['description'][:250],
                    'default_audio_language': snippet.get('defaultAudioLanguage', None),            
                    'published_at': snippet['publishedAt'],
                    'channel_id': snippet['channelId'],
                    'viewCount': statistics['viewCount'],
                    'likeCount': statistics['likeCount'],
                    'commentCount': statistics.get('commentCount', 0),
                    'video_duration': conntent_details['duration'],
                    'tags': snippet.get('tags', None),
                }
                video_info_list.append(video_info)
                channels.add(snippet['channelId'])
        return video_info_list, channels
    

    def get_channels_info(self, channels):
        channels_info = []
        for channel in channels:
            r = self.get_service().channels().list(id=channel, part="snippet,statistics").execute()
            for item in r['items']:
                snippet = item['snippet']
                statistics = item['statistics']
                channels_info.append({
                    'channel_id': item['id'],
                    'channel_title': snippet['title'],
                    'channel_description': snippet['description'],
                    'subscriber_count': statistics['subscriberCount'],
                    'video_count': statistics['videoCount'],
                    'published_at': snippet['publishedAt'],
                })
        return channels_info
    

class DatabaseLoader:

    @staticmethod
    def connect_to_database():
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_ADDRESS'),
                database=os.getenv('DB_NAME'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD')
            )
            return conn
        except psycopg2.OperationalError as e:
            print(f"Ошибка при подключении к базе данных: {e}")
            return None
    
    def get_last_dag_run(self):
        conn = self.connect_to_database()
        cursor = conn.cursor()
        select_query = f"SELECT MAX(start_date)  FROM data_mart.dag_run;"
        cursor.execute(select_query)
        response = cursor.fetchone()
        conn.close()
        return response[0] if response else None 
    
    def insert_data(self, table_name, data):
        conn = self.connect_to_database()
        if conn is None:
            return
        try:
            cursor = conn.cursor()
            placeholders = ', '.join(['%({})s'.format(key) for key in data.keys()])
            columns = ', '.join(data.keys())
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
            cursor.execute(insert_query, data)
            conn.commit()
        except Exception as e:
            print(f"Ошибка при вставке данных: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    
    def load_videos(self, video_info):
        for video in video_info:
            self.insert_data('staging.videos', video)
    
    
    def load_channels(self, channel_info):
        for channel in channel_info:
            self.insert_data('staging.channels', channel)
    
    def load_queries(self, query_info):
        for query, video_ids in query_info.items():
            for video_id in video_ids:
                self.insert_data('staging.queries', {'query': query, 'video_id': video_id})
    

if __name__ == "__main__":
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
        queries = config["queries"]
    
    loader = DatabaseLoader()
    extractor = YouTubeDataExtractor()
    last_dag_run = loader.get_last_dag_run()
    video_dict = extractor.search_videos(queries, last_dag_run=last_dag_run)
    video_info, channels = extractor.get_videos_info(video_dict)
    channels_info = extractor.get_channels_info(channels)
    loader.load_videos(video_info)
    loader.load_channels(channels_info)
    loader.load_queries(video_dict)
