from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from airflow.hooks import  MemcacheHook
from  etl_tasks_functions import get_time
from  etl_tasks_functions import subtract_time
from subdags.interference_utility import calculate_cambium_all_ss_dl_interference
from subdags.interference_utility import calculate_cambium_ss_ul_interference
from subdags.interference_utility import calculate_cambium_i_and_m_ss_ul_interference
from subdags.interference_utility import calculate_cambium_bs_interference
from subdags.interference_utility import calculate_cambium_bs_dl_interference
from subdags.interference_utility import calculate_cambiumi_bs_interference
from subdags.interference_utility import calculate_cambiumm_bs_interference
from subdags.interference_utility import backtrack_x_min
from subdags.interference_utility import get_severity_values
from subdags.interference_utility import calculate_age
from subdags.interference_utility import calculate_severity
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



redis_hook_12 = RedisHook(redis_conn_id="redis_hook_12")

DEBUG_MODE = False
down_devices = []
set_dependency_for_ss_on_all_machines = False
memc_con_cluster = MemcacheHook(memc_cnx_id = 'memc_cnx')
vrfprv_memc_con  = MemcacheHook(memc_cnx_id = 'vrfprv_memc_cnx')
pub_memc_con  = MemcacheHook(memc_cnx_id = 'pub_memc_cnx')

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
interference_service_mapping = eval(Variable.get("interference_services_mapping"))
kpi_rules = eval(Variable.get("kpi_rules"))
device_to_service_mapper = eval(Variable.get("interference_dl_kpi_to_formula_mapping"))
def process_interference_dl_kpi(
parent_dag_name, 
child_dag_name,
 start_date,
  schedule_interval,
  celery_queue,
  bs_tech_sites,
  ss_tech_sites,
  hostnames_per_site,
  hostnames_ss_per_site,
  bs_name,
  ss_name,
  config_sites): #here config site is list of all sites in system_config var
	
	try:
		
		sites = bs_tech_sites
		ss_sites = ss_tech_sites

		union_sites = set(bs_tech_sites).union(set(ss_tech_sites))
		
	except Exception:
		logging.info("Missed Data for the relevant device type ")
		traceback.print_exc()
	interference_kpi_subdag_dag = DAG(
			dag_id="%s.%s"%(parent_dag_name, child_dag_name),
			schedule_interval=schedule_interval,
			start_date=start_date,
		)
	def get_calculated_ss_data():
		ss_data = redis_hook_12.rget("calculated_ss_interference_kpi")
		combined_site_data = {}
		for site_data  in ss_data:
			site_data = eval(site_data)
			combined_site_data.update(site_data)

		return combined_site_data

	def format_bs_data(**kwargs):
		machine_name = kwargs.get("params").get("machine_name")
	
		device_type = kwargs.get("params").get("technology")
		
		bs_data = redis_hook_12.rget("calculated_bs_interference_%s_%s"%(device_type,machine_name))
		
		bs_kpi_dict = {
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

		cur_processing_time = backtrack_x_min(time.time(),300) +120 # this is used to rewind the time to previous multiple of 5 value so that kpi can be shown accordingly
		bs_devices_list = []
		#{'wimax_bs_interference_kpi': '404', 'connectedss': {1: ['10.170.72.33', '10.170.72.39'], 2: ['10.170.72.40', '10.170.72.11', '10.170.72.31', '10.170.72.58', '10.170.72.20', '10.170.72.47', '10.170.72.52', '10.170.72.56', '10.170.72.61']}, 'device_type': 'StarmaxIDU', 'services': ['wimax_bs_interference_kpi'], 'pmp1_sec': '00:0a:10:08:02:41', 'hostname': '28455', 'ipaddress': '10.170.72.2', 'pmp2_sec': '00:0a:10:08:02:43', 'site': 'ospf1_slave_1'}

		for bs_device in bs_data:

			bs_device= eval(bs_device)
			hostname = bs_device.get('hostname')
			
			
			bs_kpi_dict['machine_name']= machine_name
			bs_kpi_dict['check_timestamp']=cur_processing_time
			bs_kpi_dict['sys_timestamp']=cur_processing_time
			bs_kpi_dict['device_name']=bs_device.get('hostname')
			bs_kpi_dict['site_name']= bs_device.get('site')
			bs_kpi_dict['ip_address']=bs_device.get('ipaddress')
			

			for service in bs_device.get('services'):
				print bs_device.get('services')
				thresholds = get_severity_values(service)
				bs_kpi_dict['critical_threshold']=thresholds[0]
				bs_kpi_dict['warning_threshold']= thresholds[1]
				bs_kpi_dict['service_name']= service

				
				for data_source_sec in bs_device.get(service):
					print 1 , data_source_sec,service
					#data_source = 'interference'
					data_source = "ul_interference" if "ul_interference" in service else "dl_interference"
					bs_kpi_dict['data_source']=data_source
					bs_kpi_dict['current_value']= bs_device.get(service).get(data_source_sec)
					bs_kpi_dict['refer']=bs_device.get(data_source+'_sec')
					bs_kpi_dict['severity']= calculate_severity(service,bs_kpi_dict['current_value'])
					bs_kpi_dict['age']= calculate_age(hostname,bs_kpi_dict['severity'],bs_device.get('device_type'),cur_processing_time)
					bs_kpi_dict['min_value']= bs_kpi_dict['current_value']
					bs_kpi_dict['max_value']=bs_kpi_dict['current_value']
					bs_kpi_dict['avg_value']=bs_kpi_dict['current_value']

					

				if bs_kpi_dict['current_value'] not in ERROR_FOR_DEVICE_OMITTED:
					bs_devices_list.append(bs_kpi_dict.copy())	
				 	
				
		
		try:			
			redis_hook_12.rpush("formatted_bs_%s_%s"%(device_type,machine_name),bs_devices_list)
		except Exception:
			logging.error("Unable to push Formatted BS Data")		 
			

	#To create SS dict
	def format_ss_data(**kwargs):
		machine_name = kwargs.get("params").get("machine_name")
		device_type = kwargs.get("params").get("technology")
		
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

		ss_data =redis_hook_12.rget("calculated_ss_interference_%s_%s"%(device_type,machine_name))
		cur_processing_time = backtrack_x_min(time.time(),300) + 120 # this is used to rewind the time to previous multiple of 5 value so that kpi can be shown accordingly
		ss_devices_list = []
		for ss_device in ss_data:

			ss_device = eval(ss_device)
			
			hostname = ss_device.get('hostname')
			
			for service in ss_device.get('services'):
				if int(ss_device.get(service)) != 404:
					current_value=ss_device.get(service)
					data_source = "ul_interference" if "ul_interference" in service else "dl_interference"
					thresholds = get_severity_values(service)
					ss_kpi_dict['critical_threshold']=thresholds[0]
					ss_kpi_dict['data_source']=data_source
					ss_kpi_dict['site_name']=ss_device.get('site')
					ss_kpi_dict['severity']= 'ok' if ss_device.get(service) == 0 or ss_device.get(service) == 1 else 'unknown'  #TODO: ok and unknown are only 2 sev for ss we can incluudethis in rules later 
					ss_kpi_dict['avg_value']=current_value
					ss_kpi_dict['service_name']= service
					ss_kpi_dict['age']= calculate_age(hostname,ss_kpi_dict['severity'],ss_device.get('device_type'),cur_processing_time)
					ss_kpi_dict['min_value']= current_value
					ss_kpi_dict['machine_name']= machine_name
					ss_kpi_dict['check_timestamp']=cur_processing_time
					ss_kpi_dict['device_name']=ss_device.get('hostname')
					ss_kpi_dict['sys_timestamp']=cur_processing_time
					ss_kpi_dict['max_value']=current_value
					ss_kpi_dict['current_value']=current_value
					ss_kpi_dict['refer']=''
					ss_kpi_dict['ip_address']=ss_device.get('ipaddress')
					ss_kpi_dict['warning_threshold']= thresholds[1]
					ss_devices_list.append(ss_kpi_dict.copy())
				else:
					logging.warning("Unable to get the kpi value for device %s for %s"%(hostname,service))
					continue


		try:
			
			if len(ss_devices_list) > 0:
				redis_hook_12.rpush("formatted_ss_%s_%s"%(device_type,machine_name),ss_devices_list)
			else:
				logging.info("No %s device found in %s after formatting "%(device_type,machine_name))
		except Exception:
			logging.error("Unable to push formatted SS data to redis")
			

	def get_required_data_ss(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		machine_name = site_name.split("_")[0]
		ss_data_dict = {}
		all_ss_data = []
		if "vrfprv" in site_name:			
			memc_con = vrfprv_memc_con
				
		elif "pub" in site_name:
			memc_con = pub_memc_con
		else:
			memc_con = memc_con_cluster

		if site_name not in hostnames_ss_per_site.keys():
			logging.warning("No SS devices found for %s"%(site_name))
			return 1
		
		for hostnames_dict in hostnames_ss_per_site.get(site_name):
			host_name = hostnames_dict.get("hostname")
			ip_address = hostnames_dict.get("ip_address")
			ss_data_dict['hostname'] = host_name
			ss_data_dict['ipaddress'] = ip_address
			ss_data_dict['site_name'] = site_name
	
		
			for service in interference_service_mapping.get(ss_name):
				ss_data_dict[service] = memc_con.get("%s_%s"%(ip_address,service))
				
			all_ss_data.append(ss_data_dict.copy())
		
		print 3
		if len(all_ss_data) == 0:
			logging.error("No data Fetched ! Aborting Successfully")
			
			return 0

		try:

			print "Success %s_%s"%(device_type,machine_name)
			redis_hook_12.rpush("%s_%s"%(device_type,machine_name),all_ss_data)
			print "Success"
		except Exception:
			logging.error("Unable to insert ss data into redis")
		

		
	def get_required_data_bs(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		machine_name = site_name.split("_")[0]
		bs_data_dict = {}
		all_bs_data = []

		if "vrfprv" in site_name:			
			memc_con = vrfprv_memc_con
				
		elif "pub" in site_name:
			memc_con = pub_memc_con
		else:
			memc_con = memc_con_cluster

			
		try:
			for hostnames_dict in hostnames_per_site.get(site_name):
				
				host_name = hostnames_dict.get("hostname")
				ip_address = hostnames_dict.get("ip_address")
				

				connected_ss =  memc_con.get("%s_active_ss"%(host_name))
				bs_data_dict['hostname'] = host_name
				bs_data_dict['ipaddress'] = ip_address
				bs_data_dict['connectedss'] = connected_ss
				bs_data_dict['device_type'] = device_type
				bs_data_dict['site_name'] = site_name

				for service in interference_service_mapping.get(bs_name):
					bs_data_dict[service] = memc_con.get("%s_%s"%(host_name,service))	

				all_bs_data.append(bs_data_dict.copy())
		except TypeError:
			logging.info("Unable to get site in the hostnames_per_site  variable")
		if len(all_bs_data) > 0 :
			redis_hook_12.rpush("%s_%s"%(device_type,machine_name),all_bs_data)
		else:
			logging.info("No Host Found at site")
		
				
	def calculate_interference_data_bs(**kwargs):
		machine_name = kwargs.get("params").get("machine_name")
		device_type = kwargs.get("params").get("technology")
		services = device_to_service_mapper.get(device_type)
		ss_services = device_to_service_mapper.get(ss_name)
		ss_data = get_calculated_ss_data()
		bs_data = redis_hook_12.rget("%s_%s"%(device_type,machine_name))
		all_bs_calculated_data = []
		bs_services = interference_service_mapping.get(bs_name)
		count =0

		for base_statations in bs_data:
			devices = eval(base_statations)
			devices['site'] = devices.get('site_name')
			devices['device_type'] = device_type

			for service in services:
				if 'services' in devices.keys():
					devices['services'].append(service)
				else:
					devices['services'] = [service]

				if kpi_rules.get(service).get('isFunction'):

					devices[service] = eval(kpi_rules.get(service).get('formula'))			
				else:
					devices[service] = eval(kpi_rules.get(service).get('formula'))


				
			#IGNORING ERRORED DEVICES
			if devices.get('services'):
				all_bs_calculated_data.append(devices.copy())


		
		if len(all_bs_calculated_data) > 0:
			try:
				redis_hook_12.rpush("calculated_bs_interference_%s_%s"%(device_type,machine_name),all_bs_calculated_data)
			except Exception:
				logging.error("Unable to insert data in redis")
		else:
			logging.info("No Data found for site %s"%(machine_name))

		#here we will only calculate the interference for the bs which is dependent on the SS





	def calculate_interference_data_ss(**kwargs):
		machine_name = kwargs.get("params").get("machine_name")
		device_type = kwargs.get("params").get("technology")
		
		devices_data_dict = redis_hook_12.rget("%s_%s"%(device_type,machine_name))
		if len(devices_data_dict) == 0:
			logging.info("No Data found for ss %s "%(machine_name))
			return 1

		services = device_to_service_mapper.get(device_type)		
		ip_ul_mapper = {}
		ss_interference_list= []
		ss_data = []
		for devices in devices_data_dict:
			devices = eval(devices)
			devices['site'] = devices.get('site_name')
			devices['device_type'] = device_type


			for service in services: #loop for the all the configured services

				if 'services' in devices.keys():
					
					devices['services'].append(service)
				else:
					
					devices['services'] = []
					devices.get('services').append(service)
				
				

				if kpi_rules.get(service).get('isFunction'):
					
					devices[service] = eval(kpi_rules.get(service).get('formula'))
									
				else:
					devices[service] = eval(kpi_rules.get(service).get('formula'))

			
			ss_data.append(devices.copy())
			ip_ul_mapper[devices.get('ipaddress')] = devices.copy()
			
			
		ss_interference_list.append(ip_ul_mapper.copy())
		redis_hook_12.rpush("calculated_ss_interference_%s_%s"%(device_type,machine_name),ss_data)
		redis_hook_12.rpush("calculated_ss_interference_kpi",ss_interference_list)

	def aggregate_interference_data(*args,**kwargs):
		
		machine_name = kwargs.get("params").get("machine_name")
		
		bs_or_ss = kwargs.get("params").get("type")
		if bs_or_ss == "bs":
			device_type = bs_name
		else:
			device_type = ss_name

		#device_type = kwargs.get("params").get("device_type")
		formatted_data=redis_hook_12.rget("formatted_%s_%s_%s"%(bs_or_ss,device_type,machine_name))
		machine_data = []
		
		for site_data in formatted_data:
			machine_data.append(eval(site_data))

		redis_hook_12.set("aggregated_interference_%s_%s_%s"%(machine_name,bs_or_ss,device_type),str(machine_data))

	machine_names = set([site.split("_")[0] for site in union_sites])
	config_machines = set([site.split("_")[0] for site in config_sites])
	aggregate_dependency_ss = {}
	aggregate_dependency_bs = {}
	ss_calc_task_list = {}
	bs_calc_task_list = {}
	
	#TODo Remove this if ss >> bs task
	# calculate_interference_lost_ss_bs_task = PythonOperator(
	# 			task_id = "calculate_bs_interference_lost_ss",
	# 			provide_context=True,
	# 			python_callable=calculate_interference_data_bs,
	# 			params={"lost_n_found":True},
	# 			dag=interference_kpi_subdag_dag
	# 			)

	for each_machine_name in machine_names:
		if each_machine_name in config_machines:

			aggregate_interference_data_ss_task = PythonOperator(
				task_id = "aggregate_interference_ss_%s"%each_machine_name,
				provide_context=True,
				python_callable=aggregate_interference_data,
				params={"machine_name":each_machine_name,'type':'ss'},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue
				)
			aggregate_interference_data_bs_task = PythonOperator(
				task_id = "aggregate_interference_bs_%s"%each_machine_name,
				provide_context=True,
				python_callable=aggregate_interference_data,
				params={"machine_name":each_machine_name,'type':'bs'},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue
				)
			calculate_utilization_data_ss_task = PythonOperator(
				task_id = "calculate_ss_interference_kpi_of_%s"%each_machine_name,
				provide_context=True,
				trigger_rule = 'all_done',
				python_callable=calculate_interference_data_ss,
				params={"machine_name":each_machine_name,"technology":ss_name},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue,
				
				)
			calculate_utilization_data_bs_task = PythonOperator(
				task_id = "calculate_bs_interference_kpi_of_%s"%each_machine_name,
				provide_context=True,
				python_callable=calculate_interference_data_bs,
				trigger_rule = 'all_done',
				params={"machine_name":each_machine_name,"technology":bs_name,"lost_n_found":False},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue,
				
				)
			format_data_ss_task = PythonOperator(
				task_id = "format_data_of_ss_%s"%each_machine_name,
				provide_context=True,
				python_callable=format_ss_data,
				trigger_rule = 'all_done',
				params={"machine_name":each_machine_name,"technology":ss_name},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue,
				
				)
			format_data_bs_task = PythonOperator(
				task_id = "format_data_of_bs_%s"%each_machine_name,
				provide_context=True,
				python_callable=format_bs_data,
				trigger_rule = 'all_done',
				params={"machine_name":each_machine_name,"technology":bs_name},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue,
				
				)
			
			format_data_ss_task >> aggregate_interference_data_ss_task
			format_data_bs_task >> aggregate_interference_data_bs_task
			ss_calc_task_list[each_machine_name] = calculate_utilization_data_ss_task
			bs_calc_task_list[each_machine_name] = calculate_utilization_data_bs_task
			calculate_utilization_data_ss_task >> format_data_ss_task
			calculate_utilization_data_bs_task >> format_data_bs_task
			device_tech = {'ss':ss_name,'bs':bs_name}
			
			#we gotta create teh crazy queries WTF this is so unsafe

			INSERT_QUERY = INSERT_HEADER%("nocout_"+each_machine_name) + INSERT_TAIL
			UPDATE_QUERY = UPDATE_HEADER%("nocout_"+each_machine_name) + UPDATE_TAIL
			INSERT_QUERY = INSERT_QUERY.replace('\n','')
			UPDATE_QUERY = UPDATE_QUERY.replace('\n','')

			for bs_or_ss in device_tech.keys():
				if not DEBUG_MODE:
					insert_data_in_mysql = MySqlLoaderOperator(
						task_id ="upload_%s_data_%s"%(bs_or_ss,each_machine_name),
						dag=interference_kpi_subdag_dag,
						query=INSERT_QUERY,
						#data="",
						redis_key="aggregated_interference_%s_%s_%s"%(each_machine_name,bs_or_ss,device_tech.get(bs_or_ss)),
						redis_conn_id = "redis_hook_12",
						mysql_conn_id='mysql_uat',
						queue = celery_queue,
						trigger_rule = 'all_done'
						)
					update_data_in_mysql = MySqlLoaderOperator(
						task_id ="update_%s_data_%s"%(bs_or_ss,each_machine_name),
						query=UPDATE_QUERY	,
						#data="",
						redis_key="aggregated_interference_%s_%s_%s"%(each_machine_name,bs_or_ss,device_tech.get(bs_or_ss)),
						redis_conn_id = "redis_hook_12",
						mysql_conn_id='mysql_uat',
						dag=interference_kpi_subdag_dag,
						queue = celery_queue,
						trigger_rule = 'all_done'
						)
					if bs_or_ss == "ss":
						update_data_in_mysql << aggregate_interference_data_ss_task
						insert_data_in_mysql << aggregate_interference_data_ss_task
					else:
						update_data_in_mysql << aggregate_interference_data_bs_task
						insert_data_in_mysql << aggregate_interference_data_bs_task
				else:
					logging.info("Not inserting data Debug mode active")

	for each_site_name in union_sites:
		if each_site_name in config_sites:
			
			get_required_data_ss_task = PythonOperator(
				task_id = "get_interference_data_of_ss_%s"%each_site_name,
				provide_context=True,
				trigger_rule = 'all_done',
				python_callable=get_required_data_ss,
				params={"site_name":each_site_name,"technology":ss_name},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue
				)
			get_required_data_bs_task = PythonOperator(
				task_id = "get_interference_data_of_bs_%s"%each_site_name,
				provide_context=True,
				trigger_rule = 'all_done',
				python_callable=get_required_data_bs,
				params={"site_name":each_site_name,"technology":bs_name},
				dag=interference_kpi_subdag_dag,
				queue = celery_queue,
				
				)
			
			machine_name = each_site_name.split("_")[0]
			get_required_data_ss_task >> ss_calc_task_list.get(machine_name)			
			get_required_data_bs_task >> bs_calc_task_list.get(machine_name)

			
			

		
		else:
			logging.info("Skipping %s"%(each_site_name))



	if set_dependency_for_ss_on_all_machines:
		for bs in bs_calc_task_list:
			for ss in ss_calc_task_list:
				try:
					bs_task = bs_calc_task_list.get(bs)
					ss_task =ss_calc_task_list.get(ss)
					#print "%s << %s"%(bs,ss)
					bs_task << ss_task
				except:
					#print "EXCEPTION %s << %s"%(bs,ss)
					print "Exception"
					pass
	else:
		for bs in bs_calc_task_list:
			try:
				bs_task = bs_calc_task_list.get(bs)
				ss_task =ss_calc_task_list.get(bs)
				#print "%s << %s"%(bs,ss)
				bs_task << ss_task
			except:
				#print "EXCEPTION %s << %s"%(bs,ss)
				print "Exception"
				pass	
	

	return interference_kpi_subdag_dag



