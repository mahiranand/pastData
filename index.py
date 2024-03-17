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
        # print(key)
        if df is None or df.empty:
            # print(f"DataFrame for {key} is empty")
            continue

        if 'timestamp' in df.columns:
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce', utc=True)
            except pd.errors.OutOfBoundsDatetime:
                raise ValueError(f"Out-of-bounds nanosecond timestamp error occurred in {key} DataFrame. Please check the 'timestamp' column.")
        else:
            raise ValueError("DataFrame does not contain 'timestamp' column")
        
        if key == "ACTIVITY_CLICKED":
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
        
        elif key == "BACK_BUTTON_CLICKED":
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)

        elif key == "BUTTON_CLICKED":
            df['campaign_details_reward_amount'] = pd.to_numeric(df['campaign_details_reward_amount'], errors='coerce').fillna(0).astype(float)
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['optional_payload_stepsCompleted'] = pd.to_numeric(df['optional_payload_stepsCompleted'], errors='coerce').fillna(0).astype(float)

        elif key == "CAMPAIGN_BANNER_CLICKED":
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)
        
        elif(key == "CAMPAIGN_PLAY"):
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)

        elif(key == "COUPON_CODE_COPIED"):
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)

        elif(key == "GAME_PLAYED"):
            df['campaign_details_reward_amount'] = pd.to_numeric(df['campaign_details_reward_amount'], errors='coerce').fillna(0).astype(float)
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['optional_payload_stepsCompleted'] = pd.to_numeric(df['optional_payload_stepsCompleted'], errors='coerce').fillna(0).astype(float)
            df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].fillna(0)
        
        elif(key == "QUIZ_QUESTION_ANSWERED"):
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
            df['session_time'] = pd.to_numeric(df['session_time'], errors='coerce').fillna(0).astype(int)
            df['interaction_details_quiz_details_is_correct'] = pd.to_numeric(df['interaction_details_quiz_details_is_correct'], errors='coerce').fillna(0).astype(int)
            df['interaction_details_quiz_details_question_index'] = pd.to_numeric(df['interaction_details_quiz_details_question_index'], errors='coerce').fillna(0).astype(int)

        elif(key == "SURVEY_ANSWERED"):
            df['campaign_details_selected_slot_index'] = pd.to_numeric(df['campaign_details_selected_slot_index'], errors='coerce').fillna(0).astype(int)
        
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
            
            if( key == 'ACTIVITY_CLICKED'):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_state', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'  

            elif(key == 'BACK_BUTTON_CLICKED'):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details', 'client', 'userId', 'eventId', 'headers', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_state']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "BUTTON_CLICKED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_reward_amount', 'optional_payload_stepsCompleted', 'optional_payload_activityId', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "CAMPAIGN_BANNER_CLICKED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "CAMPAIGN_PLAY"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform','optional_payload_gratification_id', 'interaction_details', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "COLLAPSE_PIP_VIDEO"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "COUPON_CODE_COPIED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "ENTRY_POINT_CLICK"):
                rkeys = ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action_action_type', 'entry_point_data_entry_point_action_open_container', 'entry_point_data_entry_point_action_open_content_type', 'entry_point_data_entry_point_action_open_content_campaign_id', 'entry_point_data_entry_point_action_open_content_static_url', 'campaign_id', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "ENTRY_POINT_DISMISS"):
                rkeys = ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action', 'campaign_id', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "ENTRY_POINT_LOAD"):
                rkeys = ['analytics_version', 'timestamp', 'event_id', 'user_id', 'event_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_sdk_version', 'entry_point_data_entry_point_id', 'entry_point_data_entry_point_name', 'entry_point_data_entry_point_location', 'entry_point_data_entry_point_content_type', 'entry_point_data_entry_point_content_campaign_id', 'entry_point_data_entry_point_content_static_url', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_action_action_type', 'entry_point_data_entry_point_action_open_container', 'entry_point_data_entry_point_action_open_content_type', 'entry_point_data_entry_point_action_open_content_campaign_id', 'entry_point_data_entry_point_action_open_content_static_url', 'campaign_id', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "GAME_PLAYED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'campaign_details_campaign_state', 'campaign_details_reward_amount', 'platform_details_device_type', 'platform_details_os', 'optional_payload_stepsCompleted', 'optional_payload_activityId','optional_payload_gratification_id','platform_details_agent_type', 'platform_details_app_platform', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "PIP_ENTRY_POINT_CLICK"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "PIP_ENTRY_POINT_CTA_CLICK"):
                rkeys = ['entry_point_data_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_id', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_name', 'webview_content_entry_point_container','entry_point_data_entry_point_location', 'entry_point_data_entry_point_is_expanded', 'entry_point_data_entry_point_container', 'entry_point_data_entry_point_name', 'event_id', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "PIP_ENTRY_POINT_DISMISS"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "PIP_ENTRY_POINT_LOAD"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "PIP_VIDEO_25_COMPLETED"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "PIP_VIDEO_50_COMPLETED"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "PIP_VIDEO_75_COMPLETED"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "PIP_VIDEO_COMPLETED"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "QUIZ_QUESTION_ANSWERED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_quiz_details_session_id', 'interaction_details_quiz_details_question_index', 'interaction_details_quiz_details_user_answer', 'interaction_details_quiz_details_correct_answer', 'interaction_details_quiz_details_is_correct', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "SURVEY_ANSWERED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_survey_details_survey_page_index', 'interaction_details_survey_details_all_fields_0_field_type', 'interaction_details_survey_details_all_fields_0_survey_question_index', 'interaction_details_survey_details_all_fields_0_question', 'interaction_details_survey_details_all_fields_0_user_answers_0', 'interaction_details_survey_details_formId', 'interaction_details_survey_details_formSectionId', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "UNMUTE_PIP_VIDEO"):
                rkeys = ['event_id', 'webview_content_entry_point_id', 'webview_content_entry_point_location', 'webview_content_entry_point_is_expanded', 'webview_content_entry_point_container', 'webview_content_entry_point_name', 'user_id', 'session_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "VIEW_ALL"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "VIEW_REWARD_CLICKED"):
                rkeys = ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'
            
            elif(key == "WEBVIEW_DISMISS"):
                rkeys = ['event_id', 'webview_content_absolute_height', 'webview_content_campaignId', 'webview_content_webview_layout', 'webview_content_relative_height', 'webview_content_webview_url', 'user_id', 'event_name', 'dismiss_trigger', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = 'null'

            elif(key == "WEBVIEW_LOAD"):
                rkeys = ['event_id', 'webview_content_absolute_height', 'webview_content_campaignId', 'webview_content_webview_layout', 'webview_content_relative_height', 'webview_content_webview_url', 'user_id', 'event_name', 'type', 'analytics_version', 'platform_details_app_platform', 'platform_details_os', 'platform_details_sdk_version', 'platform_details_device_type', 'campaign_id', 'timestamp', 'client', 'userId', 'eventId', 'headers']
                for rkey in rkeys:
                    if rkey in one_data:
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
    feb_path = 'fe-track/2024-02-'

    for i in range(12, 12):
        i = "{:02d}".format(i)

        print(i)
        for hour in range (12, 24):
            print(hour)
            hour = "{:02d}".format(hour)

            directory_path = feb_path + i + '/' + hour

            json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
            filtered_data = makeSchema(json_data)
            store_data_into_clickhouse(client, filtered_data)
        
        # for hour in range (0, 24):
        #     print(hour)
        #     hour = "{:02d}".format(hour)

        #     directory_path = feb_path + i + '/' + hour

        #     json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
        #     filtered_data = makeSchema(json_data)
        #     store_data_into_clickhouse(client, filtered_data)
    
    mar_path = 'fe-track/2024-03-'
    
    for i in range(1, 15):
        i = "{:02d}".format(i)

        print(i)
        for hour in range (0, 24):
            print(hour)
            hour = "{:02d}".format(hour)

            directory_path = mar_path + i + '/' + hour

            json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
            filtered_data = makeSchema(json_data)
            store_data_into_clickhouse(client, filtered_data)