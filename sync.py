import os
import socket
from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.operators import BranchPythonOperator
from airflow.hooks import RedisHook
from airflow.hooks.mysql_hook import MySqlHook
from datetime import datetime, timedelta	
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
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

#################################################################DAG CONFIG####################################################################################

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 03, 30,13,00),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
PARENT_DAG_NAME = "SYNC"
main_etl_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once')
#################################################################FUCTIONS####################################################################################
def process_host_mk():
	path = Variable.get("hosts_mk_path")
	hosts = {}
	all_list = []
	device_dict = {}
	start = 0
	try:
		text_file = open(path, "r")
		
	except IOError:
		logging.error("File Name not correct")
		return "notify"
	except Exception:
		logging.error("Please check the HostMK file exists on the path provided ")
		return "notify"

	lines = text_file.readlines()
	for line in lines:
	    if "all_hosts" in line:
	  		start = 1

	    if start == 1:
	        hosts["hostname"] = line.split("|")[0]
	        hosts["device_type"] = line.split("|")[1]
	        all_list.append(hosts.copy())
	        if ']\n' in line:
	        	start = 0
	        	all_list[0]['hostname'] = all_list[0].get("hostname").strip('all_hosts += [\'')
	        	break

	if len(all_list) > 1:
	   	for device in all_list:
	  		device_dict[device.get("hostname").strip().strip("'")] = device.get("device_type").strip()
		Variable.set("hostmk.dict",str(device_dict))
		return 0
	else:
		return -4



##################################################################TASKS#########################################################################3
initiate_etl = PythonOperator(
    task_id="Initiate",
    provide_context=False,
    python_callable=process_host_mk,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)




##################################################################END#########################################################################3

