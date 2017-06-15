from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.operators import MemcToMySqlOperator
from airflow.operators import RedisToMySqlOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from subdags.topology_utility import process_topology_data
from etl_tasks_functions import get_from_socket
import json
import logging
import traceback
import tempfile
import os
from os import listdir
from os.path import isfile, join
from airflow.hooks import  MemcacheHook
import time
import math
import sys

import itertools
#TODO: Create operator changed from previous 

default_args = {
	'owner': 'wireless',
	'depends_on_past': False,
	'start_date': datetime.now(),
	'email': ['vipulsharma144@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 1,
	'retry_delay': timedelta(minutes=1),
	'provide_context': True,
	'catchup': False,
	# 'queue': 'bash_queue',
	# 'pool': 'backfill',
	# 'priority_weight': 10,
	# 'end_date': datetime(2016, 1, 1),
}
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
TOPO_DB_COLUMNS = "device_name,service_name,data_source,machine_name,site_name,ip_address,mac_address,sector_id,connected_device_ip,connected_device_mac,sys_timestamp,check_timestamp,age,refer"
topo_query = "INSERT INTO performance_topology "
topo_query += "( device_name,service_name,data_source,machine_name,site_name,ip_address,mac_address,sector_id,connected_device_ip,connected_device_mac,sys_timestamp,check_timestamp,refer) VALUES (%(device_name)s,%(service_name)s,%(data_source)s,%(machine_name)s,%(site_name)s,%(ip_address)s,%(mac_address)s,%(sector_id)s,%(connected_device_ip)s,%(connected_device_mac)s,%(sys_timestamp)s,%(check_timestamp)s,%(refer)s)"  



#################################Init Global Var ends###################################################################################

def topology_etl(parent_dag_name, child_dag_name, start_date, schedule_interval):
	config = eval(Variable.get('system_config'))

	dag_subdag_topo = DAG(
			dag_id="%s.%s" % (parent_dag_name, child_dag_name),
			schedule_interval=schedule_interval,
			start_date=start_date,
 		)
	def aggregate_topo_data(**kwargs):
		site_name = kwargs.get('params').get("site-name")
		machine_name = site_name.split("_")[1]

		key = "topo_format_"+site_name +"*"
		site_keys = redis_hook_4.get_keys(key)
		topo_data = []
		for key in site_keys:
			topo_data.extend(redis_hook_4.rget(key))
		if len(topo_data) >0:
			logging.info("Total Length recieved %s for %s"%(str(len(topo_data)),key))
		
			redis_hook_4.rpush("topology_%s"%site_name,topo_data)
		else:
			logging.info("No Data found for %s"%site_name)

	def extract_and_distribute(**kwargs):
		technology = kwargs.get('params').get("technology")
		try:
			#topology_query = Variable.get('topology_wimax_query')
			device_slot = int(Variable.get("device_slot_topology_wimax"))
			if technology == "wimax":
				check_name = "wimax_topology"
			else:
				check_name = technology+"_topology_discover"
			
			topology_query =  "GET services\nColumns: host_name host_address host_state service_description service_state plugin_output\n" + \
				"Filter: service_description = %s\nOutputFormat: json\n"%check_name
			
		
		except Exception:
			logging.info("Unable to fetch Wimax Topology Query Failing Task")
			return 1

		task_site = kwargs.get('task_instance_key_str').split('_')[4:8]
		site_name = "_".join(task_site)
		site_ip = kwargs.get('params').get("ip")
		site_port = kwargs.get('params').get("port")
		logging.info("Extracting data for site"+str(site_name)+"@"+str(site_ip)+" port "+str(site_port))
		topology_data = []
		try:
			
			topology_data = json.loads(get_from_socket(site_name, topology_query,site_ip,site_port))
			
		except Exception:
			logging.error("Unable to get Network Data")
			traceback.print_exc()

			
		group_iter = [iter(topology_data)]*int(device_slot)
		device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
		i=1;		
		for slot in device_slot_data:
			redis_hook_4.rpush("topo_"+technology+"_"+site_name+"_slot_"+str(i),slot)
			i+=1 
		return True

	def format_topo_data(**kwargs):
		logging.info("Formatting site")
		technology = kwargs.get('params').get("technology")
		site_name = kwargs.get('params').get("site_name")
		device_down_list = redis_hook_4.rget("current_down_devices_%s"%site_name)
		key_list= "topo_%s_extract_%s_slot_*"%(technology,site_name)
		topo_slots = list(redis_hook_4.get_keys(key_list))
		task_slot_data = []
		for slot_key in topo_slots:
			task_slot_data.extend(redis_hook_4.rget(slot_key))

		if len(task_slot_data) > 0:
			
			try:	
				task_slot_data = json.loads(str(task_slot_data))
			except Exception:
				logging.info("Some Exception here")
				traceback.print_exc()
		else:
			logging.info("No Data Recieved for topo")

		connected_ss = process_topology_data(task_slot_data,device_down_list,technology,site_name)
		key = "topo_format_"+site_name +"_"+technology
		logging.info("Len is %s of type %s"%(len(connected_ss),type(connected_ss)))
		if len(connected_ss) > 0:
			redis_hook_4.rpush(key,connected_ss)
		else:
			logging.info("No data recieved after processing")


	for machine in config :
		for site in machine.get('sites'):
			
			
			redis_to_mysql = RedisToMySqlOperator(
				task_id="insert_topology_data_mysql_%s"%site.get('name'),	
				#python_callable = "upload_network_data_mysql",
				dag=dag_subdag_topo,
				mysql_table="performance_topology",
				redis_key="topology_"+str(site.get('name')),
				mysql_conn_id='application_db',
				exec_type = 3,
				sql=topo_query,
				update = False,
				db_coloumns = TOPO_DB_COLUMNS,
				trigger_rule = 'all_done'
				)

			aggregation_task = PythonOperator(
			task_id="aggregate_%s"%site.get('name'),
			provide_context=True,
			python_callable=aggregate_topo_data,
			params={"site-name":site.get('name')},
			trigger_rule = 'all_done',
			dag=dag_subdag_topo
			)



			for tech in ["wimax","rad5k","cambium","cam450i"]:
				extract_topology = PythonOperator(
				task_id="Topology_%s_extract_%s"%(tech,site.get('name')),
				provide_context=True,
				python_callable=extract_and_distribute,
				params={"ip":machine.get('ip'),"port":site.get('port'),"technology":tech},
				dag=dag_subdag_topo
				)
				format_topology = PythonOperator(
				task_id="Topology_%s_format_%s"%(tech,site.get('name')),
				provide_context=True,
				python_callable=format_topo_data,
				params={"technology":tech,"site_name":site.get('name')},
				dag=dag_subdag_topo
				)

				extract_topology >> format_topology
				format_topology >> aggregation_task
				
			aggregation_task >> redis_to_mysql	
		

	return dag_subdag_topo