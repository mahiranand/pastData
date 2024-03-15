from azure.storage.blob import BlobServiceClient
from clickhouse_driver import Client
from flatten_json import flatten
from dotenv import load_dotenv
import pandas as pd
import json
import os


import numpy as np

def store_data_into_clickhouse(client, data):
    for key, value in data.items():
        df = pd.DataFrame(value)

        if 'timestamp' in df.columns:
            df['timestamp'] = df['timestamp'].replace('null', np.nan)
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', utc=True)
        else:
            raise ValueError("DataFrame does not contain 'timestamp' column")
        
        if key == "PAGE_OPENED":
            df['campaign_details_reward_amount'] = pd.to_numeric(df['campaign_details_reward_amount'], errors='coerce').fillna(0)
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0)
        
        if key == "PROMPT_SHOWN":
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0)
        
        client.insert_dataframe(f'INSERT INTO {key} VALUES', df, settings=dict(use_numpy=True))


def makeSchema(json_data):
    filtered_data = {}
    for(key, value) in json_data.items():
        if(len(value) == 0):
            continue

        if(filtered_data.get(key) == None):
            filtered_data[key] = []

        for one_data in value:
            #conditions
            one_data = flatten(one_data)
            new_data = {}
            
            if( key == 'PAGE_OPENED'):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'campaign_details_reward_amount']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'  

            if( key == "PROMPT_SHOWN"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if key in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            filtered_data[key].append(new_data)
        
    return filtered_data
                
  


def read_json_data_from_azure(client, container_name, directory_path):
    container_client = client.get_container_client(container_name)
    blob_list = container_client.list_blobs(name_starts_with=directory_path)

    json_data_dist = {}

    for blob in blob_list:
        if(blob.name.endswith('.json')):
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob().readall()
            blob_data_str = blob_data.decode('utf-8')
            json_objects = blob_data_str.strip().split('\n')

            for json_obj in json_objects:
                json_data = json.loads(json_obj)
                
                if json_data_dist.get(json_data['event_name']) is None:
                    json_data_dist[json_data['event_name']] = []

                json_data_dist[json_data['event_name']].append(json_data)
    
    return json_data_dist

if __name__ == '__main__':

    load_dotenv()

    AZURE_CONNECTION_STRING = os.getenv('AZURE_CONNECTION_STRING')
    AZURE_CONTAINER = os.getenv('AZURE_CONTAINER')

    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')
    CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER')
    CLICKHOUSE_DATABASE_NAME = os.getenv('CLICKHOUSE_DATABASE_NAME')

    blob_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    client = Client(host = CLICKHOUSE_HOST, user = CLICKHOUSE_USER, password = CLICKHOUSE_PASSWORD, database = CLICKHOUSE_DATABASE_NAME)

    # json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, 'fe-page/2024-02-14/00')
    # filteredData = makeSchema(json_data)
    # store_data_into_clickhouse(client, filteredData)
    feb_path = 'fe-page/2024-02-'

    for i in range(1, 30):
        i = "{:02d}".format(i)

        print(i)
        for hour in range (0, 24):
            print(hour)
            hour = "{:02d}".format(hour)

            directory_path = feb_path + i + '/' + hour

            json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
            filtered_data = makeSchema(json_data)
            store_data_into_clickhouse(client, filtered_data)
    
    mar_path = 'fe-page/2024-03-'
    
    for i in range(1, 15):
        i = "{:02d}".format(i)

        print
        for hour in range (0, 24):
            print(hour)
            hour = "{:02d}".format(hour)

            directory_path = mar_path + i + '/' + hour

            json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
            filtered_data = makeSchema(json_data)
            store_data_into_clickhouse(client, filtered_data)