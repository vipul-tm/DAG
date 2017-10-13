from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from airflow.hooks import  MemcacheHook
from  etl_tasks_functions import get_time
from  etl_tasks_functions import subtract_time
from subdags.utilization_utility import calculate_wimax_utilization
from subdags.utilization_utility import calculate_cambium_ss_utilization
from subdags.utilization_utility import calculate_radwin5k_ss_utilization
from subdags.utilization_utility import calculate_radwin5k_bs_utilization
from subdags.utilization_utility import calculate_radwin5kjet_ss_utilization
from subdags.utilization_utility import calculate_radwin5kjet_bs_utilization
from subdags.utilization_utility import calculate_radwin5k_bs_and_ss_dyn_tl_kpi
from subdags.utilization_utility import calculate_backhaul_utilization
from subdags.utilization_utility import calculate_ptp_utilization
from subdags.utilization_utility import calculate_mrotek_utilization


from subdags.utilization_utility import backtrack_x_min
from subdags.utilization_utility import get_severity_values
from subdags.utilization_utility import calculate_age
from subdags.utilization_utility import calculate_severity
from airflow.operators import MySqlLoaderOperator
import logging
import itertools
import socket
import random
import traceback
import time
from pprint import pprint
default_args = {
	'owner': 'wireless',
	'depends_on_past': False,
	'start_date': datetime.now() - timedelta(minutes=2),
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
redis_hook_util_10 = RedisHook(redis_conn_id="redis_hook_util_10")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
INSERT_HEADER = "INSERT INTO %s.performance_utilization"
INSERT_TAIL = """
(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) 
values 
(%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s)

 """

UPDATE_HEADER = "INSERT INTO %s.performance_utilizationstatus"
UPDATE_TAIL = """
(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer )
 values
  (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s) 
  ON DUPLICATE KEY UPDATE machine_name = VALUES(machine_name),current_value = VALUES(current_value),age=VALUES(age),site_name=VALUES(site_name),critical_threshold=VALUES(critical_threshold),severity=VALUES(severity),sys_timestamp=VALUES(sys_timestamp),ip_address=VALUES(ip_address),warning_threshold=VALUES(warning_threshold),check_timestamp=VALUES(check_timestamp),refer=VALUES(refer)
"""
ERROR_DICT ={404:'Device not found yet',405:'No SS Connected to BS-BS is not skipped'}
ERROR_FOR_DEVICE_OMITTED = [404]
kpi_rules = eval(Variable.get("kpi_rules"))
sv_to_ds_mapping = {}

def process_utilization_kpi(
parent_dag_name, 
child_dag_name,
 start_date,
  schedule_interval,
  celery_queue,
  ss_tech_sites,
  hostnames_ss_per_site,
  ss_name,
  utilization_attributes,
  config_sites): #here config site is list of all sites in system_config var
	

	utilization_kpi_subdag_dag = DAG(
			dag_id="%s.%s"%(parent_dag_name, child_dag_name),
			schedule_interval=schedule_interval,
			start_date=start_date,
		)
	for service in utilization_attributes:
		sv_to_ds_mapping [service.get("service_name")] ={"data_source":service.get("data_source"),"sector_type":service.get("sector_type")}

	def get_calculated_ss_data():
		ss_data = redis_hook_util_10.rget("calculated_ss_utilization_kpi")
		combined_site_data = {}
		for site_data  in ss_data:
			site_data = eval(site_data)
			combined_site_data.update(site_data)

		return combined_site_data

	#To create SS dict
	def format_data(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		utilization_attributes = kwargs.get("params").get("attributes")
		machine_name = ""
		ss_kpi_dict = {
				'site_name': 'unknown' ,
				'device_name': 'unknown',
				'service_name': 'unknown',
				'ip_address': 'unknown',
				'severity': 'unknown',
				'age': 'unknown',
				'data_source': 'unknown',
				'current_value': 'unknown',
				'warning_threshold': 'unknown',
				'critical_threshold': 'unknown',
				'check_timestamp': 'unknown',
				'sys_timestamp': 'unknown' ,
				'refer':'unknown',
				'min_value':'unknown',
				'max_value':'unknown',
				'avg_value':'unknown',
				'machine_name':'unknown'
				}

		ss_data =redis_hook_util_10.rget("calculated_utilization_%s_%s"%(device_type,site_name))
		cur_processing_time = backtrack_x_min(time.time(),300) # this is used to rewind the time to previous multiple of 5 value so that kpi can be shown accordingly
		ss_devices_list = []
		pprint(sv_to_ds_mapping)
		for ss_device in ss_data:
			ss_device = eval(ss_device)
			hostname = ss_device.get('hostname')
			machine_name = ss_device.get('site').split("_")[0]

			for service in ss_device.get('services'):
				 
				data_source = sv_to_ds_mapping.get(service).get("data_source")
				pmp_type = sv_to_ds_mapping.get(service).get("sector_type")
				thresholds = get_severity_values(service)
				ss_kpi_dict['critical_threshold']=thresholds[0]
				ss_kpi_dict['data_source']=data_source
				ss_kpi_dict['site_name']=ss_device.get('site')
				  #TODO: ok and unknown are only 2 sev for ss we can incluudethis in rules later 				
				ss_kpi_dict['service_name']= service		
				
				ss_kpi_dict['machine_name']= machine_name
				ss_kpi_dict['check_timestamp']=cur_processing_time
				ss_kpi_dict['device_name']=ss_device.get('hostname')
				ss_kpi_dict['sys_timestamp']=cur_processing_time			
				ss_kpi_dict['refer']=ss_device.get("%s_sector"%(pmp_type))
				ss_kpi_dict['ip_address']=ss_device.get('ipaddress')
				ss_kpi_dict['warning_threshold']= thresholds[1]
				
				if not isinstance(ss_device.get(service),dict):
					ss_kpi_dict['severity']= calculate_severity(service,ss_device.get(service))
					ss_kpi_dict['age']= calculate_age(hostname,ss_kpi_dict['severity'],ss_device.get('device_type'),cur_processing_time,service)
					ss_kpi_dict['current_value']=ss_device.get(service)
					ss_kpi_dict['avg_value']=ss_kpi_dict['current_value']
					ss_kpi_dict['min_value']=ss_kpi_dict['current_value']
					ss_kpi_dict['max_value']=ss_kpi_dict['current_value']

					if ss_kpi_dict['current_value'] != None:
						ss_devices_list.append(ss_kpi_dict.copy())
				else:
					for data_source in ss_device.get(service):
						ds_values = ss_device.get(service)
						ss_kpi_dict['data_source']=data_source
						ss_kpi_dict['severity']= calculate_severity(service,ds_values.get(data_source))
						ss_kpi_dict['age']= calculate_age(hostname,ss_kpi_dict['severity'],ss_device.get('device_type'),cur_processing_time,service)
						ss_kpi_dict['current_value'] = ss_device.get(service).get(data_source)
						ss_kpi_dict['avg_value']=ss_device.get(service).get(data_source)
						ss_kpi_dict['min_value']=ss_device.get(service).get(data_source)
						ss_kpi_dict['max_value']=ss_device.get(service).get(data_source)
						if ss_kpi_dict['current_value'] != None:
							ss_devices_list.append(ss_kpi_dict.copy())

		try:
			
			if len(ss_devices_list) > 0:
				redis_hook_util_10.rpush("formatted_util_%s_%s"%(device_type,machine_name),ss_devices_list)
			else:
				logging.info("No %s device found in %s after formatting "%(device_type,machine_name))
		except Exception:
			logging.error("Unable to push formatted SS data to redis")
			

	def get_required_data_ss(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		utilization_attributes = kwargs.get("params").get("attributes")
		ss_data_dict = {}
		all_ss_data = []
		if site_name not in hostnames_ss_per_site.keys():
			logging.warning("No SS devices found for %s"%(site_name))
			return 1

		for hostnames_dict in hostnames_ss_per_site.get(site_name):
			host_name = hostnames_dict.get("hostname")
			ip_address = hostnames_dict.get("ip_address")
			ss_data_dict['hostname'] = host_name
			ss_data_dict['ipaddress'] = ip_address
	
			
			for service in utilization_attributes:			
				ss_data_dict[service.get('service_name')] = memc_con.get(service.get('utilization_key')%(host_name))
			
			all_ss_data.append(ss_data_dict.copy())

		if len(all_ss_data) == 0:
			logging.info("No data Fetched ! Aborting Successfully")
			return 0
		try:
			redis_hook_util_10.rpush("%s_%s"%(device_type,site_name),all_ss_data)
		except Exception:
			logging.warning("Unable to insert ss data into redis")
		

		#pprint(all_ss_data)


	def calculate_utilization_data_ss(**kwargs):

		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		utilization_attributes = kwargs.get("params").get("attributes")

		devices_data_dict = redis_hook_util_10.rget("%s_%s"%(device_type,site_name))
		if len(devices_data_dict) == 0:
			logging.info("No Data found for ss %s "%(site_name))
			return 1

		
		
		ss_data = []
		for devices in devices_data_dict:
			devices = eval(devices)
			devices['site'] = site_name
			devices['device_type'] = device_type

			for service_attributes in utilization_attributes: #loop for the all the configured services
				service  = service_attributes.get('service_name')

				if service_attributes.get('isKpi'):
					if 'services' in devices.keys() and devices.get('services') != None:
						devices.get('services').append(service)
					elif service and  devices.get('services') == None:
						devices['services'] = [service]
					else:
						
						devices['services'] = []

				
				if service_attributes.get('isKpi'):
					utilization_type = service_attributes.get("utilization_type")
					capacity = None
					if "capacity" in service_attributes.keys():
						capacity =  service_attributes.get("capacity")

					devices[service] = eval(kpi_rules.get(service).get('formula'))
									
				else:
					continue

			#ip_ul_mapper[devices.get('ipaddress')] = devices
			ss_data.append(devices.copy())
			
		#ss_utilization_list.append(ip_ul_mapper.copy())
		redis_hook_util_10.rpush("calculated_utilization_%s_%s"%(device_type,site_name),ss_data)
		#redis_hook_util_10.rpush("calculated_ss_utilization_kpi",ss_utilization_list)

	def aggregate_utilization_data(*args,**kwargs):
		print "Aggregating Data"
		machine_name = kwargs.get("params").get("machine_name")
		device_type = kwargs.get("params").get("technology")
		
		#device_type = kwargs.get("params").get("device_type")
		formatted_data=redis_hook_util_10.rget("formatted_util_%s_%s"%(device_type,machine_name))
		machine_data = []
		
		for site_data in formatted_data:
			machine_data.append(eval(site_data))

		redis_hook_util_10.set("aggregated_utilization_%s_%s"%(machine_name,device_type),str(machine_data))

	machine_names = set([site.split("_")[0] for site in ss_tech_sites])
	config_machines = set([site.split("_")[0] for site in config_sites])
	aggregate_dependency_ss = {}
	aggregate_dependency_bs = {}
	ss_calc_task_list = []
	bs_calc_task_list = []
	
	#TODo Remove this if ss >> bs task
	# calculate_utilization_lost_ss_bs_task = PythonOperator(
	# 			task_id = "calculate_bs_utilization_lost_ss",
	# 			provide_context=True,
	# 			python_callable=calculate_utilization_data_bs,
	# 			params={"lost_n_found":True},
	# 			dag=utilization_kpi_subdag_dag
	# 			)

	for each_machine_name in machine_names:
		if each_machine_name in config_machines:

			aggregate_utilization_data_ss_task = PythonOperator(
				task_id = "aggregate_utilization_ss_%s"%each_machine_name,
				provide_context=True,
				python_callable=aggregate_utilization_data,
				params={"machine_name":each_machine_name,"technology":ss_name},
				dag=utilization_kpi_subdag_dag,
				queue = celery_queue
				)
			aggregate_dependency_ss[each_machine_name] = aggregate_utilization_data_ss_task

			
			
			#we gotta create teh crazy queries WTF this is so unsafe

			INSERT_QUERY = INSERT_HEADER%("nocout_"+each_machine_name) + INSERT_TAIL
			UPDATE_QUERY = UPDATE_HEADER%("nocout_"+each_machine_name) + UPDATE_TAIL
			INSERT_QUERY = INSERT_QUERY.replace('\n','')
			UPDATE_QUERY = UPDATE_QUERY.replace('\n','')

			#ss_name == Device_type
			insert_data_in_mysql = MySqlLoaderOperator(
				task_id ="upload_data_%s"%(each_machine_name),
				dag=utilization_kpi_subdag_dag,
				query=INSERT_QUERY,
				#data="",
				redis_key="aggregated_utilization_%s_%s"%(each_machine_name,ss_name),
				redis_conn_id = "redis_hook_util_10",
				mysql_conn_id='mysql_uat',
				queue = celery_queue,
				trigger_rule = 'all_done'
				)
			update_data_in_mysql = MySqlLoaderOperator(
				task_id ="update_data_%s"%(each_machine_name),
				query=UPDATE_QUERY	,
				#data="",
				redis_key="aggregated_utilization_%s_%s"%(each_machine_name,ss_name),
				redis_conn_id = "redis_hook_util_10",
				mysql_conn_id='mysql_uat',
				dag=utilization_kpi_subdag_dag,
				queue = celery_queue,
				trigger_rule = 'all_done'
				)
		
			update_data_in_mysql << aggregate_utilization_data_ss_task
			insert_data_in_mysql << aggregate_utilization_data_ss_task


	for each_site_name in ss_tech_sites:
		if each_site_name in config_sites:
			
			get_required_data_ss_task = PythonOperator(
				task_id = "get_utilization_data_of_ss_%s"%each_site_name,
				provide_context=True,
				trigger_rule = 'all_done',
				python_callable=get_required_data_ss,
				params={"site_name":each_site_name,"technology":ss_name,'attributes':utilization_attributes},
				dag=utilization_kpi_subdag_dag,
				queue = celery_queue
				)

			calculate_utilization_data_ss_task = PythonOperator(
				task_id = "calculate_ss_utilization_kpi_of_%s"%each_site_name,
				provide_context=True,
				trigger_rule = 'all_done',
				python_callable=calculate_utilization_data_ss,
				params={"site_name":each_site_name,"technology":ss_name,'attributes':utilization_attributes},
				dag=utilization_kpi_subdag_dag,
				queue = celery_queue,
				
				)

			format_data_ss_task = PythonOperator(
				task_id = "format_data_of_ss_%s"%each_site_name,
				provide_context=True,
				python_callable=format_data,
				trigger_rule = 'all_done',
				params={"site_name":each_site_name,"technology":ss_name,'attributes':utilization_attributes},
				dag=utilization_kpi_subdag_dag,
				queue = celery_queue,
				
				)

			

			get_required_data_ss_task >> calculate_utilization_data_ss_task
			calculate_utilization_data_ss_task >> format_data_ss_task
			#calculate_utilization_data_ss_task >> calculate_utilization_data_bs_task
			ss_calc_task_list.append(calculate_utilization_data_ss_task)
			
			
			

			machine_name = each_site_name.split("_")[0]
			try:
				
				aggregate_dependency_ss[machine_name] << format_data_ss_task
				
			except:
				logging.info("Site Not Found %s"%(machine_name))
				pass

		
		else:
			logging.info("Skipping %s"%(each_site_name))


	#print bs_calc_task_list 
	#print ss_calc_task_list

	
	return utilization_kpi_subdag_dag

