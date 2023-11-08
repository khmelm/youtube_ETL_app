import json
import pendulum


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule

from datetime import timedelta, datetime
from googleapiclient.discovery import build
from dotenv import load_dotenv

from utils.youtube import YouTubeDataExtractor, DatabaseLoader

load_dotenv()

DEFAULT_ARGS = {
    'retries': 1,
    'execution_timeout': timedelta(minutes=5),
}

DAG_PARAMS = {
    'dag_id': 'dag_dwh',
    'default_args': DEFAULT_ARGS,
    'start_date': pendulum.today('UTC').add(days=-2),
    'schedule_interval': '00 12 * * *',
    'max_active_runs': 1,
    'tags': ['extract_youtube_data', 'analytical_report', 'load_data']
}

YOUTUBE = 'youtube'

class EtlBuilder:
    def __init__(self, task_id: str):
        self.task_id = task_id

    def run(self):
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
    
    def build(self) -> PythonOperator:
        return PythonOperator(task_id=self.task_id, provide_context=True, python_callable=self.run)


with DAG(**DAG_PARAMS) as dag:
    dag.doc_md = __doc__
    t_process = EtlBuilder(task_id='process').build()

@task
def load_channels():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_channels()')

t_load_channels = load_channels()

@task
def load_channel_dates():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_dates(''staging'', ''channels'')')

t_load_channel_dates = load_channel_dates()

@task
def load_video_dates():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_dates(''staging'', ''videos'')')

t_load_video_dates = load_video_dates()

@task
def load_lang():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_lang()')

t_load_lang = load_lang()

@task
def load_query():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_query()')

t_load_query = load_query()

@task
def load_videos():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_videos()')

t_load_videos= load_videos()

@task
def load_video_query():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_video_query()')

t_load_video_query = load_video_query()

@task
def load_video_stats():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_video_stats()')

t_load_video_stats = load_video_stats()

@task
def load_channel_stats():
    target_hook = PostgresHook(YOUTUBE)
    target_hook.run('select data_mart.load_channel_stats()')

t_load_channel_stats = load_channel_stats()

t_process >> t_load_channels >> t_load_channel_dates >> t_load_video_dates >> t_load_lang >> t_load_query >> t_load_videos >> t_load_video_query >> t_load_video_stats >> t_load_channel_stats





