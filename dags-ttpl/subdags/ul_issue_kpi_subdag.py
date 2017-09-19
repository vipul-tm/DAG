from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from airflow.hooks import  MemcacheHook
from  etl_tasks_functions import get_time
from  etl_tasks_functions import subtract_time
from subdags.ul_issue_utility import calculate_wimax_ss_ul_issue
from subdags.ul_issue_utility import calculate_wimax_bs_ul_issue
from subdags.ul_issue_utility import calculate_cambium_bs_ul_issue
from subdags.ul_issue_utility import calculate_cambium_ss_ul_issue
from subdags.ul_issue_utility import calculate_radwin5k_bs_ul_issue
from subdags.ul_issue_utility import calculate_radwin5k_ss_ul_issue
from subdags.ul_issue_utility import backtrack_x_min
from subdags.ul_issue_utility import get_severity_values
from subdags.ul_issue_utility import calculate_age
from subdags.ul_issue_utility import calculate_severity
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

redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4") #number specifies the DB in Use
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
redis_hook_6 = RedisHook(redis_conn_id="redis_hook_6")
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
ul_issue_service_mapping = eval(Variable.get("ul_issue_services_mapping"))
kpi_rules = eval(Variable.get("kpi_rules"))
device_to_service_mapper = eval(Variable.get("ul_issue_kpi_to_formula_mapping"))
def process_ul_issue_kpi(
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
	ul_issue_kpi_subdag_dag = DAG(
			dag_id="%s.%s"%(parent_dag_name, child_dag_name),
			schedule_interval=schedule_interval,
			start_date=start_date,
		)
	def get_calculated_ss_data():
		ss_data = redis_hook_6.rget("calculated_ss_ul_issue_kpi")
		combined_site_data = {}
		for site_data  in ss_data:
			site_data = eval(site_data)
			combined_site_data.update(site_data)

		return combined_site_data

	def format_bs_data(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		machine_name = site_name.split("_")[0]
		device_type = kwargs.get("params").get("technology")
		print "calculated_bs_ul_issue_%s_%s"%(machine_name,device_type)
		bs_data = redis_hook_6.rget("calculated_bs_ul_issue_%s_%s"%(device_type,site_name))
		print bs_data
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

		cur_processing_time = backtrack_x_min(time.time(),300) # this is used to rewind the time to previous multiple of 5 value so that kpi can be shown accordingly
		bs_devices_list = []
		#{'wimax_bs_ul_issue_kpi': '404', 'connectedss': {1: ['10.170.72.33', '10.170.72.39'], 2: ['10.170.72.40', '10.170.72.11', '10.170.72.31', '10.170.72.58', '10.170.72.20', '10.170.72.47', '10.170.72.52', '10.170.72.56', '10.170.72.61']}, 'device_type': 'StarmaxIDU', 'services': ['wimax_bs_ul_issue_kpi'], 'pmp1_sec': '00:0a:10:08:02:41', 'hostname': '28455', 'ipaddress': '10.170.72.2', 'pmp2_sec': '00:0a:10:08:02:43', 'site': 'ospf1_slave_1'}

		for bs_device in bs_data:
			bs_device= eval(bs_device)
			hostname = bs_device.get('hostname')
			machine = bs_device.get('site').split("_")[0]

			bs_kpi_dict['machine_name']= machine
			bs_kpi_dict['check_timestamp']=cur_processing_time
			bs_kpi_dict['sys_timestamp']=cur_processing_time
			bs_kpi_dict['device_name']=bs_device.get('hostname')
			bs_kpi_dict['site_name']= bs_device.get('site')
			bs_kpi_dict['ip_address']=bs_device.get('ipaddress')
			bs_kpi_dict['max_value']=0
			bs_kpi_dict['avg_value']=0

			for service in bs_device.get('services'):
				thresholds = get_severity_values(service)
				bs_kpi_dict['critical_threshold']=thresholds[0]
				bs_kpi_dict['warning_threshold']= thresholds[1]
				bs_kpi_dict['service_name']= service

				bs_kpi_dict['min_value']= 0
				
				for data_source in bs_device.get(service):
					#data_source = 'ul_issue'
					bs_kpi_dict['data_source']=data_source+"_ul_issue" if device_type == "StarmaxIDU" else 'ul_issue'
					bs_kpi_dict['current_value']= bs_device.get(service).get(data_source)
					bs_kpi_dict['refer']=bs_device.get(data_source+'_sec')
					bs_kpi_dict['severity']= calculate_severity(service,bs_kpi_dict['current_value'])
					bs_kpi_dict['age']= calculate_age(hostname,bs_kpi_dict['severity'],bs_device.get('device_type'),cur_processing_time)
					if bs_kpi_dict['current_value'] not in ERROR_DICT.keys():
						bs_devices_list.append(bs_kpi_dict.copy())	
				 	
				
		pprint (bs_devices_list)
		try:
			redis_hook_6.rpush("formatted_bs_%s_%s"%(device_type,machine_name),bs_devices_list)
		except Exception:
			logging.error("Unable to push Formatted BS Data")		 
			

	#To create SS dict
	def format_ss_data(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
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

		ss_data =redis_hook_6.rget("calculated_ss_ul_issue_%s_%s"%(device_type,site_name))
		cur_processing_time = backtrack_x_min(time.time(),300) # this is used to rewind the time to previous multiple of 5 value so that kpi can be shown accordingly
		ss_devices_list = []
		for ss_device in ss_data:
			ss_device = eval(ss_device)
			hostname = ss_device.get('hostname')
			machine_name = ss_device.get('site').split("_")[0]
			for service in ss_device.get('services'):
				data_source = 'ul_issue'
				thresholds = get_severity_values(service)
				ss_kpi_dict['critical_threshold']=thresholds[0]
				ss_kpi_dict['data_source']=data_source
				ss_kpi_dict['site_name']=ss_device.get('site')
				ss_kpi_dict['severity']= 'ok' if ss_device.get(service) == 0 or ss_device.get(service) == 1 else 'unknown'  #TODO: ok and unknown are only 2 sev for ss we can incluudethis in rules later 
				ss_kpi_dict['avg_value']=0
				ss_kpi_dict['service_name']= service
				ss_kpi_dict['age']= calculate_age(hostname,ss_kpi_dict['severity'],ss_device.get('device_type'),cur_processing_time)
				ss_kpi_dict['min_value']= 0
				ss_kpi_dict['machine_name']= machine_name
				ss_kpi_dict['check_timestamp']=cur_processing_time
				ss_kpi_dict['device_name']=ss_device.get('hostname')
				ss_kpi_dict['sys_timestamp']=cur_processing_time
				ss_kpi_dict['max_value']=0
				ss_kpi_dict['current_value']=ss_device.get(service)
				ss_kpi_dict['refer']=''
				ss_kpi_dict['ip_address']=ss_device.get('ipaddress')
				ss_kpi_dict['warning_threshold']= thresholds[1]
				ss_devices_list.append(ss_kpi_dict.copy())

		try:
			print len(ss_devices_list)
			if len(ss_devices_list) > 0:
				redis_hook_6.rpush("formatted_ss_%s_%s"%(device_type,machine_name),ss_devices_list)
			else:
				logging.info("No %s device found in %s after formatting "%(device_type,machine_name))
		except Exception:
			logging.error("Unable to push formatted SS data to redis")
			

	def get_required_data_ss(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		ss_data_dict = {}
		all_ss_data = []
		print "+++++++++++++++++++++++++"
		print len(hostnames_ss_per_site.get(site_name))
		print "+++++++++++++++++++++++++"
		for hostnames_dict in hostnames_ss_per_site.get(site_name):
			host_name = hostnames_dict.get("hostname")
			ip_address = hostnames_dict.get("ip_address")
			ss_data_dict['hostname'] = host_name
			ss_data_dict['ipaddress'] = ip_address
	
			
			for service in ul_issue_service_mapping.get(ss_name):			
				ss_data_dict[service] = memc_con.get("%s_%s"%(ip_address,service))
			
			all_ss_data.append(ss_data_dict.copy())

		redis_hook_6.rpush("%s_%s"%(device_type,site_name),all_ss_data)
		

		#pprint(all_ss_data)

	def get_required_data_bs(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		bs_data_dict = {}
		all_bs_data = []
		for hostnames_dict in hostnames_per_site.get(site_name):
			host_name = hostnames_dict.get("hostname")
			ip_address = hostnames_dict.get("ip_address")
			connected_ss =  memc_con.get("%s_conn_ss"%(host_name))
			bs_data_dict['hostname'] = host_name
			bs_data_dict['ipaddress'] = ip_address
			bs_data_dict['connectedss'] = connected_ss
			bs_data_dict['device_type'] = device_type

			for service in ul_issue_service_mapping.get(bs_name):
				bs_data_dict[service] = memc_con.get("%s_%s"%(host_name,service))	

			all_bs_data.append(bs_data_dict.copy())
		redis_hook_6.rpush("%s_%s"%(device_type,site_name),all_bs_data)
		
				
	def calculate_ul_issue_data_bs(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")
		devices_data_dict = redis_hook_6.rget("%s_%s"%(device_type,site_name))
		services = device_to_service_mapper.get(device_type)
		ss_services = device_to_service_mapper.get(ss_name)
		ss_data = get_calculated_ss_data()
		bs_data = redis_hook_6.rget("%s_%s"%(device_type,site_name))
		all_bs_calculated_data = []
		bs_services = ul_issue_service_mapping.get(bs_name)
		count =0

		for base_statations in bs_data:
			devices = eval(base_statations)
			devices['site'] = site_name
			devices['device_type'] = device_type

			for service in services:
				if 'services' in devices.keys():
					devices['services'] = devices.get('services').append(service)
				else:
					devices['services'] = [service]

				if kpi_rules.get(service).get('isFunction'):
					devices[service] = eval(kpi_rules.get(service).get('formula'))			
				else:
					devices[service] = eval(kpi_rules.get(service).get('formula'))

			#IGNORING ERRORED DEVICES
			if devices.get('services'):
				for service_check in devices.get('services'):
					if devices.get(service_check) not in ERROR_DICT.keys():
						all_bs_calculated_data.append(devices.copy())


		
			# print dev.get('wimax_bs_ul_issue_kpi')
			# if dev.get('wimax_bs_ul_issue_kpi') != '404' and (dev.get('wimax_bs_ul_issue_kpi').get('pmp1') > 0 or dev.get('wimax_bs_ul_issue_kpi').get('pmp2') > 0):
			# 	print dev
			# elif dev.get('wimax_bs_ul_issue_kpi') == '404':
			# 	count=count+1
			# else:
			# 	print "%s-%s"%(dev.get('wimax_bs_ul_issue_kpi').get('pmp1'),dev.get('wimax_bs_ul_issue_kpi').get('pmp2'))
		
		
		redis_hook_6.rpush("calculated_bs_ul_issue_%s_%s"%(device_type,site_name),all_bs_calculated_data)
		#here we will only calculate the ul_issue for the bs which is dependent on the SS





	def calculate_ul_issue_data_ss(**kwargs):
		site_name = kwargs.get("params").get("site_name")
		device_type = kwargs.get("params").get("technology")

		devices_data_dict = redis_hook_6.rget("%s_%s"%(device_type,site_name))
		services = device_to_service_mapper.get(device_type)
		ip_ul_mapper = {}
		ss_ul_issue_list= []
		ss_data = []
		for devices in devices_data_dict:
			devices = eval(devices)
			devices['site'] = site_name
			devices['device_type'] = device_type

			for service in services: #loop for the all the configured services

				if 'services' in devices.keys():
					devices['services'] = devices.get('services').append(service)
				else:
					devices['services'] = [service]

				if kpi_rules.get(service).get('isFunction'):
					devices[service] = eval(kpi_rules.get(service).get('formula'))
									
				else:
					devices[service] = eval(kpi_rules.get(service).get('formula'))

			ip_ul_mapper[devices.get('ipaddress')] = devices
			ss_data.append(devices.copy())
			
		ss_ul_issue_list.append(ip_ul_mapper.copy())
		redis_hook_6.rpush("calculated_ss_ul_issue_%s_%s"%(device_type,site_name),ss_data)
		redis_hook_6.rpush("calculated_ss_ul_issue_kpi",ss_ul_issue_list)

	def aggregate_ul_issue_data(*args,**kwargs):
		print "Aggregating Data"
		machine_name = kwargs.get("params").get("machine_name")
		
		bs_or_ss = kwargs.get("params").get("type")
		if bs_or_ss == "bs":
			device_type = bs_name
		else:
			device_type = ss_name

		#device_type = kwargs.get("params").get("device_type")
		formatted_data=redis_hook_6.rget("formatted_%s_%s_%s"%(bs_or_ss,device_type,machine_name))
		machine_data = []
		
		for site_data in formatted_data:
			machine_data.append(eval(site_data))

		redis_hook_6.set("aggregated_ul_issue_%s_%s_%s"%(machine_name,bs_or_ss,device_type),str(machine_data))

	machine_names = set([site.split("_")[0] for site in union_sites])
	config_machines = set([site.split("_")[0] for site in config_sites])
	aggregate_dependency_ss = {}
	aggregate_dependency_bs = {}
	ss_calc_task_list = []
	bs_calc_task_list = []
	
	#TODo Remove this if ss >> bs task
	# calculate_ul_issue_lost_ss_bs_task = PythonOperator(
	# 			task_id = "calculate_bs_ul_issue_lost_ss",
	# 			provide_context=True,
	# 			python_callable=calculate_ul_issue_data_bs,
	# 			params={"lost_n_found":True},
	# 			dag=ul_issue_kpi_subdag_dag
	# 			)

	for each_machine_name in machine_names:
		if each_machine_name in config_machines:

			aggregate_ul_issue_data_ss_task = PythonOperator(
				task_id = "aggregate_ul_issue_ss_%s"%each_machine_name,
				provide_context=True,
				python_callable=aggregate_ul_issue_data,
				params={"machine_name":each_machine_name,'type':'ss'},
				dag=ul_issue_kpi_subdag_dag
				)
			aggregate_ul_issue_data_bs_task = PythonOperator(
				task_id = "aggregate_ul_issue_bs_%s"%each_machine_name,
				provide_context=True,
				python_callable=aggregate_ul_issue_data,
				params={"machine_name":each_machine_name,'type':'bs'},
				dag=ul_issue_kpi_subdag_dag
				)	
			aggregate_dependency_ss[each_machine_name] = aggregate_ul_issue_data_ss_task
			aggregate_dependency_bs[each_machine_name] = aggregate_ul_issue_data_bs_task
			device_tech = {'ss':ss_name,'bs':bs_name}
			
			#we gotta create teh crazy queries WTF this is so unsafe

			INSERT_QUERY = INSERT_HEADER%("nocout_"+each_machine_name) + INSERT_TAIL
			UPDATE_QUERY = UPDATE_HEADER%("nocout_"+each_machine_name) + UPDATE_TAIL
			INSERT_QUERY = INSERT_QUERY.replace('\n','')
			UPDATE_QUERY = UPDATE_QUERY.replace('\n','')

			for bs_or_ss in device_tech.keys():
				insert_data_in_mysql = MySqlLoaderOperator(
					task_id ="upload_%s_data_%s"%(bs_or_ss,each_machine_name),
					dag=ul_issue_kpi_subdag_dag,
					query=INSERT_QUERY,
					#data="",
					redis_key="aggregated_ul_issue_%s_%s_%s"%(each_machine_name,bs_or_ss,device_tech.get(bs_or_ss)),
					redis_conn_id = "redis_hook_6",
					mysql_conn_id='mysql_uat'
					)
				update_data_in_mysql = MySqlLoaderOperator(
					task_id ="update_%s_data_%s"%(bs_or_ss,each_machine_name),
					query=UPDATE_QUERY,
					#data="",
					redis_key="aggregated_ul_issue_%s_%s_%s"%(each_machine_name,bs_or_ss,device_tech.get(bs_or_ss)),
					redis_conn_id = "redis_hook_6",
					mysql_conn_id='mysql_uat',
					dag=ul_issue_kpi_subdag_dag,
					)
				if bs_or_ss == "ss":
					update_data_in_mysql << aggregate_ul_issue_data_ss_task
					insert_data_in_mysql << aggregate_ul_issue_data_ss_task
				else:
					update_data_in_mysql << aggregate_ul_issue_data_bs_task
					insert_data_in_mysql << aggregate_ul_issue_data_bs_task

	for each_site_name in union_sites:
		if each_site_name in config_sites:
			
			get_required_data_ss_task = PythonOperator(
				task_id = "get_ul_issue_data_of_ss_%s"%each_site_name,
				provide_context=True,
				python_callable=get_required_data_ss,
				params={"site_name":each_site_name,"technology":ss_name},
				dag=ul_issue_kpi_subdag_dag
				)
			get_required_data_bs_task = PythonOperator(
				task_id = "get_ul_issue_data_of_bs_%s"%each_site_name,
				provide_context=True,
				python_callable=get_required_data_bs,
				params={"site_name":each_site_name,"technology":bs_name},
				dag=ul_issue_kpi_subdag_dag
				)
			calculate_utilization_data_ss_task = PythonOperator(
				task_id = "calculate_ss_ul_issue_kpi_of_%s"%each_site_name,
				provide_context=True,
				python_callable=calculate_ul_issue_data_ss,
				params={"site_name":each_site_name,"technology":ss_name},
				dag=ul_issue_kpi_subdag_dag
				)
			calculate_utilization_data_bs_task = PythonOperator(
				task_id = "calculate_bs_ul_issue_kpi_of_%s"%each_site_name,
				provide_context=True,
				python_callable=calculate_ul_issue_data_bs,
				params={"site_name":each_site_name,"technology":bs_name,"lost_n_found":False},
				dag=ul_issue_kpi_subdag_dag
				)
			format_data_ss_task = PythonOperator(
				task_id = "format_data_of_ss_%s"%each_site_name,
				provide_context=True,
				python_callable=format_ss_data,
				params={"site_name":each_site_name,"technology":ss_name},
				dag=ul_issue_kpi_subdag_dag
				)
			format_data_bs_task = PythonOperator(
				task_id = "format_data_of_bs_%s"%each_site_name,
				provide_context=True,
				python_callable=format_bs_data,
				params={"site_name":each_site_name,"technology":bs_name},
				dag=ul_issue_kpi_subdag_dag
				)

			get_required_data_ss_task >> calculate_utilization_data_ss_task
			calculate_utilization_data_ss_task >> format_data_ss_task
			#calculate_utilization_data_ss_task >> calculate_utilization_data_bs_task
			ss_calc_task_list.append(calculate_utilization_data_ss_task)
			bs_calc_task_list.append(calculate_utilization_data_bs_task)
			calculate_utilization_data_bs_task >> format_data_bs_task
			get_required_data_bs_task >> calculate_utilization_data_bs_task

			machine_name = each_site_name.split("_")[0]
			try:
				
				aggregate_dependency_ss[machine_name] << format_data_ss_task
				aggregate_dependency_bs[machine_name] << format_data_bs_task
			except:
				logging.info("Site Not Found %s"%(machine_name))
				pass

		
		else:
			logging.info("Skipping %s"%(each_site_name))


	#print bs_calc_task_list 
	#print ss_calc_task_list

	for bs in bs_calc_task_list:
		for ss in ss_calc_task_list:
			try:
				#print "%s << %s"%(bs,ss)
				bs << ss
			except:
				#print "EXCEPTION %s << %s"%(bs,ss)
				print "Exception"
				pass
	

	return ul_issue_kpi_subdag_dag

