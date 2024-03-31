from azure.storage.blob import BlobServiceClient
from clickhouse_driver import Client
from flatten_json import flatten
from dotenv import load_dotenv
import pandas as pd
import json
import os


import numpy as np

def store_data_into_clickhouse(client, data):
    # print(data)
    for key, value in data.items():
        df = pd.DataFrame(value)
        print(key , len(value)) 
        if df is None or df.empty:
            print(f"DataFrame for {key} is empty")
            continue

        if 'timestamp' in df.columns:
            try:
                df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed', errors='coerce', utc=True)
            except pd.errors.OutOfBoundsDatetime:
                raise ValueError(f"Out-of-bounds nanosecond timestamp error occurred in {key} DataFrame. Please check the 'timestamp' column.")
        else:
            raise ValueError("DataFrame does not contain 'timestamp' column")
        
        if(key == "REWARD_CREATED"):
            df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(int, errors='ignore').fillna(0)
            df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            df['reward_details_stepsCompleted'] = df['reward_details_stepsCompleted'].astype(int, errors='ignore').fillna(0)

        if(key == "GAMECHALLENGE_ACTIVITY_COMPLETED"):
            df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_chances_credited'] = df['campaign_details_campaign_activity_activity_chances_credited'].astype(int, errors='ignore').fillna(0)
            
        if(key == "CAMPAIGN_COMPLETED"):
            df['reward_details_score'] = df['reward_details_score'].astype(float, errors='ignore').fillna(0)
            df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(float, errors='ignore').fillna(0)
            df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)

        if( key == "CAMPAIGN_EXPIRED"):
            df['event_properties_campaign_details_campaign_expired'] = df['event_properties_campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)

        if(key == "CAMPAIGN_JOINED"):
            df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
        
        if(key == "MULTISTEP_ACTIVITY_COMPLETED"):
            df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)

        if(key == "REWARD_GRANTED"):
            df['reward_details_reward_amount'] = df['reward_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            df['reward_details_reward_index'] = df['reward_details_reward_index'].astype(int, errors='ignore').fillna(0)
            df['reward_details_stepsCompleted'] = df['reward_details_stepsCompleted'].astype(int, errors='ignore').fillna(0)
        
        if(key == "ALL_REWARDS_CONSUMED"):
            df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)

        if(key == "STREAK_ACTIVITY_COMPLETED"):
            df['campaign_details_campaign_expired'] = df['campaign_details_campaign_expired'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_steps_completed'] = df['campaign_details_campaign_steps_completed'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_total_steps'] = df['campaign_details_campaign_total_steps'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_total'] = df['campaign_details_campaign_activity_activity_completed_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_completed_daily'] = df['campaign_details_campaign_activity_activity_completed_daily'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_total'] = df['campaign_details_campaign_activity_activity_limits_total'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_campaign_activity_activity_limits_daily'] = df['campaign_details_campaign_activity_activity_limits_daily'].astype(int, errors='ignore').fillna(0)

        if(key == "PAGE_OPENED"):
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
        
        if(key == "BUTTON_CLICKED"):
            df['optional_payload_userResponse'] = df['optional_payload_userResponse'].astype(str, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(float, errors='ignore').fillna(0)
            df['optional_payload_score'] = df['optional_payload_score'].astype(int, errors='ignore').fillna(0)
        
        if(key == "BACK_BUTTON_CLICKED"):
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_score'] = df['optional_payload_score'].astype(int, errors='ignore').fillna(0)
        
        if(key == "GAME_PLAYED"):
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(float, errors='ignore').fillna(0)
            df['optional_payload_score'] = df['optional_payload_score'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_quiz_details_score'] = df['interaction_details_quiz_details_score'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_quiz_details_percentage'] = df['interaction_details_quiz_details_percentage'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_quiz_details_all_answered_correctly'] = df['interaction_details_quiz_details_all_answered_correctly'].astype(int, errors='ignore').fillna(0)

        if(key == "PROMPT_SHOWN"):
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(float, errors='ignore').fillna(0)
        
        if(key == "SURVEY_ANSWERED"):
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_survey_details_survey_page_index'] = df['interaction_details_survey_details_survey_page_index'].astype(int, errors='ignore').fillna(0)
        
        if(key == "ACTIVITY_CLICKED"):
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)

        if(key == "VIEW_ALL"):
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
        
        if(key == "COUPON_CODE_COPIED"):
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_stepsCompleted'] = df['optional_payload_stepsCompleted'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_score'] = df['optional_payload_score'].astype(int, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(int, errors='ignore').fillna(0)
        
        if(key == "CAMPAIGN_PLAY"):
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(int, errors='ignore').fillna(0)
        
        if(key == "CAMPAIGN_BANNER_CLICKED"):
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['interaction_details'] = df['interaction_details'].astype(str, errors='ignore').fillna(0)
            df['campaign_details_reward_amount'] = df['campaign_details_reward_amount'].astype(int, errors='ignore').fillna(0)
            
        if(key == "WEBVIEW_DISMISS"):
            df['webview_content_absolute_height'] = df['webview_content_absolute_height'].astype(float).fillna(0).astype(int)
            df['webview_content_relative_height'] = df['webview_content_relative_height'].astype(float).fillna(0).astype(int)

        if(key == "WEBVIEW_LOAD"):
            df['webview_content_absolute_height'] = df['webview_content_absolute_height'].astype(float).fillna(0).astype(int)
            df['webview_content_relative_height'] = df['webview_content_relative_height'].astype(float).fillna(0).astype(int)
        
        if(key == "UI_INTERACTION"):
            df['event_properties'] = df['event_properties'].astype(str, errors='ignore').fillna(0)

        if(key == "QUIZ_QUESTION_ANSWERED"):
            df['campaign_details_selected_slot_index'] = df['campaign_details_selected_slot_index'].astype(int, errors='ignore').fillna(0)
            df['session_time'] = df['session_time'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_quiz_details_is_correct'] = df['interaction_details_quiz_details_is_correct'].astype(int, errors='ignore').fillna(0)
            df['interaction_details_quiz_details_question_index'] = df['interaction_details_quiz_details_question_index'].astype(int, errors='ignore').fillna(0)
            df['optional_payload_score'] = df['optional_payload_score'].astype(int, errors='ignore').fillna(0)
        
        
        
        client.insert_dataframe(f'INSERT INTO {key} VALUES', df, settings=dict(use_numpy=True))




def makeSchema(json_data):
    filtered_data = {}
    for(key, value) in json_data.items():
        if(len(value) == 0):
            continue

        if(filtered_data.get(key) == None):
            filtered_data[key] = []
        
        required_keys = {   
                        #backend events (9)
                        "REWARD_CREATED" : ["type", "event_id", "client", "user_id", "analytics_version", "timestamp", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_status", "reward_details_event_name", "reward_details_brand_name", "reward_details_series_id", "reward_details_stepsCompleted", "reward_details_activityId", "reward_details_reward_coupon_code", "reward_details_reward_index", "reward_details_reward_id", "reward_details_reward_status", "reward_details_reward_title", "reward_details_reward_body", "reward_details_reward_type", "reward_details_reward_amount", "reward_details_audiance_id", "reward_details_key", "event_name"],
                        "GAMECHALLENGE_ACTIVITY_COMPLETED": ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'campaign_details_campaign_activity_activity_chances_credited', 'event_name'],
                        "CAMPAIGN_COMPLETED" : ["type", "event_id", "user_id", "analytics_version", "timestamp", "client", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_state", "reward_details_score", "reward_details_reward_coupon_code", "reward_details_reward_index", "reward_details_reward_id", "reward_details_reward_status", "reward_details_reward_title", "reward_details_reward_body", "reward_details_reward_expiry", "reward_details_reward_type", "reward_details_reward_amount", "reward_details_ruleId", "reward_details_expiryDate", "event_name", "reward_details_userResponse", "reward_details_Cart_Value", "reward_details_expiry", "reward_details_gratification_id", "reward_details_keyasdj"],
                        "CAMPAIGN_EXPIRED" : ['analytics_version', 'client', 'event_id', 'event_name', 'event_properties_campaign_details_campaign_experience', 'event_properties_campaign_details_campaign_expiration_type', 'event_properties_campaign_details_campaign_expired', 'event_properties_campaign_details_campaign_expiry', 'event_properties_campaign_details_campaign_id', 'event_properties_campaign_details_campaign_name', 'event_properties_campaign_details_campaign_state', 'timestamp', 'user_id'],
                        "CAMPAIGN_JOINED" : ['type', 'event_id', 'user_id', 'analytics_version', 'timestamp', 'client', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'event_name'],
                        "MULTISTEP_ACTIVITY_COMPLETED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'event_name'],
                        "REWARD_GRANTED" : ["type", "event_id", "user_id", "analytics_version", "timestamp", "client", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_status", "reward_details_event_name", "reward_details_brand_name", "reward_details_series_id", "reward_details_stepsCompleted", "reward_details_activityId", "reward_details_reward_coupon_code", "reward_details_reward_index", "reward_details_reward_id", "reward_details_reward_status", "reward_details_reward_title", "reward_details_reward_body", "reward_details_reward_type", "reward_details_reward_amount", "reward_details_transaction_id", "reward_details_key", "event_name", "reward_details_audiance_id", "reward_details_is_recon_reward", "reward_details_parent_id", "reward_details_is_gratification_campaign"],
                        "STREAK_ACTIVITY_COMPLETED" : ['type', 'event_id', 'client', 'user_id', 'analytics_version', 'timestamp', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'campaign_details_campaign_activity_activity_completed_total', 'campaign_details_campaign_activity_activity_completed_daily', 'campaign_details_campaign_activity_activity_limits_total', 'campaign_details_campaign_activity_activity_limits_daily', 'campaign_details_campaign_activity_campaign_activity_status', 'campaign_details_campaign_activity_campaign_activity_id', 'campaign_details_campaign_activity_campaign_activity_event_name', 'campaign_details_campaign_activity_campaign_activity_completed_on', 'event_name'],
                        "ALL_REWARDS_CONSUMED": ['type', 'event_id', 'user_id', 'analytics_version', 'timestamp', 'client', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_campaign_status', 'campaign_details_campaign_steps_completed', 'campaign_details_campaign_total_steps', 'campaign_details_campaign_expires_on', 'campaign_details_campaign_expiry_type', 'campaign_details_campaign_expired', 'event_name'],
                        "FORM_SECTION_SUBMITTED" : ["type", "event_id", "user_id", "analytics_version", "timestamp", "client", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "form_details_form_id", "form_details_form_section_id", "event_name"],
                        
                        #entry point events (3)
                        "ENTRY_POINT_DISMISS" : ["analytics_version", "timestamp", "event_id", "user_id", "event_name", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_sdk_version", "entry_point_data_entry_point_id", "entry_point_data_entry_point_name", "entry_point_data_entry_point_location", "entry_point_data_entry_point_content_type", "entry_point_data_entry_point_content_campaign_id", "entry_point_data_entry_point_content_static_url", "entry_point_data_entry_point_container", "entry_point_data_entry_point_action_action_type", "entry_point_data_entry_point_action_open_container", "entry_point_data_entry_point_action_open_content_type", "entry_point_data_entry_point_action_open_content_campaign_id", "entry_point_data_entry_point_action_open_content_static_url", "campaign_id", "client", "userId", "eventId", "headers", "entry_point_data_entry_point_is_expanded", "type", "platform_details_app_platform"],
                        "ENTRY_POINT_LOAD" : ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_content_type", "entry_point_data_entry_point_content_campaign_id", "entry_point_data_entry_point_content_static_url", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "entry_point_data_entry_point_action_action_type", "entry_point_data_entry_point_action_open_container", "entry_point_data_entry_point_action_open_content_type", "entry_point_data_entry_point_action_open_content_campaign_id", "entry_point_data_entry_point_action_open_content_static_url", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp", "client", "userId", "eventId", "headers", "entry_point_data_entry_point_is_expanded"],
                        "ENTRY_POINT_CLICK": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_content_type", "entry_point_data_entry_point_content_campaign_id", "entry_point_data_entry_point_content_static_url", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "entry_point_data_entry_point_action_action_type", "entry_point_data_entry_point_action_open_container", "entry_point_data_entry_point_action_open_content_type", "entry_point_data_entry_point_action_open_content_campaign_id", "entry_point_data_entry_point_action_open_content_static_url", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp", "client", "userId", "eventId", "headers", "entry_point_data_entry_point_is_expanded"],
                        
                        #frontend events (15)
                        "BUTTON_CLICKED": ['optional_payload_userResponse','analytics_version', 'campaign_details_campaign_experience', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_state', 'campaign_details_coupon_code', 'campaign_details_reward_amount', 'campaign_details_reward_status', 'campaign_details_reward_title', 'campaign_details_reward_type', 'campaign_details_reward_user_id', 'campaign_details_selected_slot_index', 'client', 'eventId', 'event_id', 'event_name', 'headers', 'interaction_details_button_name', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'optional_payload_Cart_Value', 'optional_payload_activityId', 'optional_payload_expiry', 'optional_payload_expiryDate', 'optional_payload_ruleId', 'optional_payload_score', 'optional_payload_stepsCompleted', 'page_details_page_layout', 'page_details_page_name', 'platform_details_agent_type', 'platform_details_app_platform', 'platform_details_device_type', 'platform_details_os', 'referrer', 'session_id', 'session_time', 'timestamp', 'type', 'userId', 'user_id'],
                        "BACK_BUTTON_CLICKED": ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_reward_amount", "optional_payload_stepsCompleted", "optional_payload_activityId", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "interaction_details", "optional_payload_score", "optional_payload_Cart_Value", "client", "userId", "eventId", "headers"],
                        "GAME_PLAYED" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_coupon_code", "campaign_details_campaign_state", "campaign_details_reward_amount", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "client", "userId", "eventId", "headers", "optional_payload_stepsCompleted", "optional_payload_activityId", "optional_payload_gratification_id", "optional_payload_Cart_Value", "optional_payload_score", "optional_payload_ruleId", "optional_payload_expiryDate", "optional_payload_key", "optional_payload_expiry", "interaction_details_quiz_details_score", "interaction_details_quiz_details_percentage", "interaction_details_quiz_details_all_answered_correctly"],
                        "SURVEY_ANSWERED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_survey_details_survey_page_index', 'interaction_details_survey_details_formId', 'interaction_details_survey_details_formSectionId', 'client', 'userId', 'eventId', 'headers', 'campaign_details_reward_amount', 'optional_payload_Cart_Value'],
                        "ACTIVITY_CLICKED": ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_state", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "interaction_details_button_name", "client", "userId", "eventId", "headers"],
                        "VIEW_REWARD_CLICKED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_button_name', 'client', 'userId', 'eventId', 'headers'],
                        "VIEW_ALL" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "interaction_details_button_name", "client", "userId", "eventId", "headers", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_state"],
                        "COUPON_CODE_COPIED": ['type', 'event_name', 'event_id', 'user_id', 'timestamp', 'session_id', 'session_time', 'referrer', 'analytics_version', 'page_details_page_layout', 'page_details_page_name', 'campaign_details_campaign_id', 'campaign_details_campaign_name', 'campaign_details_campaign_experience', 'campaign_details_reward_user_id', 'campaign_details_reward_status', 'campaign_details_campaign_state', 'campaign_details_selected_slot_index', 'campaign_details_reward_type', 'campaign_details_reward_title', 'campaign_details_coupon_code', 'platform_details_device_type', 'platform_details_os', 'platform_details_agent_type', 'platform_details_app_platform', 'interaction_details_properties_channel', 'interaction_details_properties_code', 'interaction_details_properties_deep_link', 'interaction_details_properties_web_link', 'client', 'userId', 'eventId', 'headers', 'optional_payload_Cart_Value', 'optional_payload_stepsCompleted', 'optional_payload_activityId', 'optional_payload_score', 'campaign_details_reward_amount'],
                        "CAMPAIGN_PLAY" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "interaction_details", "optional_payload_Cart_Value", "client", "userId", "eventId", "headers", "campaign_details_reward_amount", "optional_payload_gratification_id", "optional_payload_key", "optional_payload_expiry"],
                        "CAMPAIGN_BANNER_CLICKED" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "platform_details_app_platform", "interaction_details", "client", "userId", "eventId", "headers", "campaign_details_reward_amount"],
                        "WEBVIEW_DISMISS": ["event_id", "webview_content_absolute_height", "webview_content_campaignId", "webview_content_webview_layout", "webview_content_relative_height", "webview_content_webview_url", "user_id", "session_id", "event_name", "dismiss_trigger", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp", "client", "userId", "eventId", "headers"],
                        "WEBVIEW_LOAD": ["event_id", "webview_content_absolute_height", "webview_content_campaignId", "webview_content_webview_layout", "webview_content_relative_height", "webview_content_webview_url", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp", "client", "userId", "eventId", "headers", "session_id"],
                        "UI_INTERACTION" : ['timestamp', 'event_name', 'event_properties', 'client', 'userId', 'eventId', 'headers'],
                        "QUIZ_QUESTION_ANSWERED" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "interaction_details_quiz_details_session_id", "interaction_details_quiz_details_question_index", "interaction_details_quiz_details_user_answer", "interaction_details_quiz_details_correct_answer", "interaction_details_quiz_details_is_correct", "client", "userId", "eventId", "headers", "platform_details_app_platform", "optional_payload_score"],
                        "wallet_interaction": ['timestamp', 'event_name', 'event_properties_pageName', 'event_properties_action', 'client', 'userId', 'eventId', 'headers'],
                        
                        #PAGE EVENTS (2)
                        "PAGE_OPENED" : ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_reward_amount", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "client", "userId", "eventId", "headers", "platform_details_app_platform"],
                        "PROMPT_SHOWN": ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_reward_amount", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "client", "userId", "eventId", "headers", "platform_details_app_platform"],
                        
                        #PIP EVENTS (12)
                        "PIP_ENTRY_POINT_LOAD": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp", "client", "userId", "eventId", "headers"],
                        "PIP_ENTRY_POINT_DISMISS": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_ENTRY_POINT_CLICK": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "MUTE_PIP_VIDEO": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "UNMUTE_PIP_VIDEO": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "EXPAND_PIP_VIDEO": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "COLLAPSE_PIP_VIDEO": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_VIDEO_25_COMPLETED": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_VIDEO_50_COMPLETED": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_VIDEO_75_COMPLETED": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_VIDEO_COMPLETED": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],
                        "PIP_ENTRY_POINT_CTA_CLICK": ["entry_point_data_entry_point_id", "entry_point_data_entry_point_location", "entry_point_data_entry_point_is_expanded", "entry_point_data_entry_point_container", "entry_point_data_entry_point_name", "event_id", "user_id", "session_id", "event_name", "type", "analytics_version", "platform_details_app_platform", "platform_details_os", "platform_details_sdk_version", "platform_details_device_type", "campaign_id", "timestamp","client", "userId", "eventId", "headers"],

                        #QUESTION/ANSWER TABLE
                        "QUESTION_ANSWER": ["client", "event_id", "timestamp", "user_id", "question", "user_answers", "survey_question_index", "field_type", "event_name", "campaign_details_campaign_id", "ques_id", "source_event"], 
                    }
        for one_data in value:
            #conditions
            one_data = flatten(one_data)
            new_data = {}

            # if(key == "REWARD_CREATED"):
            #     rkeys = ["type", "event_id", "client", "user_id", "analytics_version", "timestamp", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_campaign_status", "reward_details_event_name", "reward_details_brand_name", "reward_details_series_id", "reward_details_stepsCompleted", "reward_details_activityId", "reward_details_reward_coupon_code", "reward_details_reward_index", "reward_details_reward_id", "reward_details_reward_status", "reward_details_reward_title", "reward_details_reward_body", "reward_details_reward_type", "reward_details_reward_amount", "reward_details_audiance_id", "reward_details_key", "event_name"]

            
            if(required_keys.get(key) is not None):
                for rkey in required_keys[key]:
                    if one_data.get(rkey) is not None:
                        new_data[rkey] = one_data[rkey]
                    else:
                        new_data[rkey] = None
            
            # if(key == "PAGE_OPENED"):
            #     rkeys = ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_reward_amount", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "client", "userId", "eventId", "headers", "platform_details_app_platform"]

            #     for rkey in rkeys:
            #         if one_data.get(rkey) is not None:
            #             new_data[rkey] = one_data[rkey]
            #         else:
            #             new_data[rkey] = None

            # if(key == "PROMPT_SHOWN"):
            #     rkeys = ["type", "event_name", "event_id", "user_id", "timestamp", "session_id", "session_time", "referrer", "analytics_version", "page_details_page_layout", "page_details_page_name", "campaign_details_campaign_id", "campaign_details_campaign_name", "campaign_details_campaign_experience", "campaign_details_reward_user_id", "campaign_details_reward_status", "campaign_details_campaign_state", "campaign_details_selected_slot_index", "campaign_details_reward_type", "campaign_details_reward_title", "campaign_details_reward_amount", "campaign_details_coupon_code", "platform_details_device_type", "platform_details_os", "platform_details_agent_type", "client", "userId", "eventId", "headers", "platform_details_app_platform"]

            #     for rkey in rkeys:
            #         if one_data.get(rkey) is not None:
            #             new_data[rkey] = one_data[rkey]
            #         else:
            #             new_data[rkey] = None

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
                
                if(json_data.get("event_name") is None):
                    print(json_data)
                    continue

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
    jan_path = 'sdk-webview/2024-01-'

    for i in range(1, 32):
        i = "{:02d}".format(i)

        print(i)
        for hour in range (0, 24):
            print(hour)
            hour = "{:02d}".format(hour)

            directory_path = jan_path + i + '/' + hour

            json_data = read_json_data_from_azure(blob_client, AZURE_CONTAINER, directory_path)
            filtered_data = makeSchema(json_data)
            store_data_into_clickhouse(client, filtered_data)

    feb_path = 'sdk-webview/2024-02-'

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
    
    mar_path = 'sdk-webview/2024-03-'
    
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