import os
import socket
from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks import KafkaHook
from datetime import datetime, timedelta	
from airflow.models import Variable
from airflow.operators.kafka_extractor_operator import KafkaExtractorOperator
import itertools
import socket
import sys
import time
import re
import random
import logging
import traceback
import os
import json
import utility


#TODO: Commenting 
#######################################DAG CONFIG####################################################################################################################

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 04, 27,20,20),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "KAFKA"
CHILD_DAG_NAME_NETWORK = "NETWORK"
CHILD_DAG_NAME_SERVICE = "SERVICE"
CHILD_DAG_NAME_EVENTS = "EVENTS"  
CHILD_DAG_NAME_FORMAT = "FORMAT"
NW_DB_COLUMNS = "machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer"
SV_DB_COLUMNS = "machine_name,severity,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,current_value,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer"

main_etl_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once',)

#######################################DAG Config Ends ####################################################################################################################






#######################################TASKS####################################################################################################################




insert_sv_data = KafkaExtractorOperator(
    task_id="Extact_Kafka_Data",    
    #python_callable = upload_service_data_mysql,
    dag=main_etl_dag,
    redis_conn='redis_hook_4',
    kafka_con_id='kafka_default',
    topic='ml_queue',
    identifier_output='redID',
    output_identifier_index='redIDInd',
    index_key='timestamp',
#    skeleton_dict={'b':'t'},
    indexed=False,
    )
