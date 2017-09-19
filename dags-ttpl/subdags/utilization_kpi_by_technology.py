from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from airflow.hooks import  MemcacheHook

#from  etl_tasks_functions import get_time
#from  etl_tasks_functions import subtract_time

from datetime import datetime, timedelta
import logging
import itertools
import socket
import random
import traceback
import time
from ast import literal_eval

rules = eval(Variable.get('rules'))
operators = eval(Variable.get('operators')) #get operator Dict from 

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
    #'queue': 'bash_queue',
    #'pool': 'backfill',
    #'priority_weight': 10,
    #'end_date': datetime(2016, 1, 1),
}

#redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4") #number specifies the DB in Use
redis_cnx = RedisHook(redis_conn_id="redis_hook_7")
memc_cnx = MemcacheHook(memc_cnx_id = 'memc_cnx')


def process_utilization_kpi( 	parent_dag_name, 
				child_dag_name, 
				start_date, 
				schedule_interval,
				celery_queue, 
				technology, 
				devices, 
				attributes
				):

    	site_names = devices.keys()
    	machine_names = list(set([each_site.split('_')[0] for each_site in devices.keys()]))

    	#logging.info("Site names by utilization subdag(%s): %s\n"%(technology,site_names))

    	utilization_kpi_by_technology = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)

	def evaluate_condition(rules,current_value):
		result =  'False'
		result_all = []
		
		for i in range(1,len(rules),2):
			threshold_value = rules[i].get('value') #get threshold from rules dict
			operator = rules[i].get('operator') #get operator from rules
			service_name = rules[i].get('name')
			symbol = operators.get(operator) #get symbol from dict
			if threshold_value != '' and current_value != '':
				#logging.info("\n Evaluating ")
				
				#logging.info("Evaluating the Formula ---> %s%s%s of %s as %s"%(str(current_value),str(symbol),str(threshold_value) , str(service_name) ,eval("%s%s%s"%(float(current_value),str(symbol),float(threshold_value)))))
				try:
					if eval("%s%s%s"%(float(current_value),str(symbol),float(threshold_value))):
						result_all.append('True')
					else:
						result_all.append('False')
				except (NameError, SyntaxError, TypeError, ValueError):
					
					if eval('\''+str(current_value)+'\''+symbol+'\''+str(threshold_value)+'\''):
						result_all.append('True')
					else:
						result_all.append('False')
				except Exception:
					logging.info("Some WTF Exception")
					if eval('\''+str(current_value)+'\''+symbol+'\''+str(threshold_value)+'\''):
						result_all.append('True')
					else:
						result_all.append('False')
			else:
				result_all.append('False')

			try:
				#logging.info(rules)
				#logging.info("i="+str(i))
				if rules[i+1] == 'AND' or rules[i+1] == 'OR' and rules[i+1] != None:
					result_all.append(rules[i+1].lower())
				
			except IndexError:
					#logging.info('No Conjugator or the rule ended')
					continue
		#logging.info("The Result of %s After compiling booleans ---> %s"%(str(service_name),str(result_all)))
		if len(result_all) == 1:
			result = eval(result_all[0])
		elif len(result_all) % 2 != 0:
			result = eval(" ".join(result_all))

		else:

			logging.info("Please Check the syntax of rules")

		#logging.info("returning ; %s"%str(result))
		return result

	def calculate_severity(service,cur,host_state="",ds=""):

		final_severity = []
		global rules 
		
		if not (ds == "pl" and host_state == "down"):
			#TODO: Currently using loop to get teh Dic value can get hashed value provided the total severity for the devices remain fixed need to consult
			try:
		
				total_severities = rules.get(service) #TODO: Handle if service not found
				total_severities_len = len(total_severities)
			#Severity 1 will be the first to checked and should be the top priority i.e if 
			except TypeError:
				#logging.info("The specified service "+service+" does not have a rule specified in rules variable")
				return 'unknown'
			
			for i in range(1,total_severities_len+1):
					current_severity = ""
					sv_rules = total_severities.get("Severity"+str(i))
					
					if sv_rules[0]:
						current_severity = sv_rules[0]
					else:
						current_severity = 'unknown'
						logging.warning("Please provide severity name for " + str(service))

					result = evaluate_condition(sv_rules,cur)
					if result:	
						return current_severity
						#final_severity =  final_severity.append(evaluate_condition(rules,cur)) #Later can be used to get all the SEV and then based upon priority decide Severity
						#logging.info("The Final Result for Service "+service+" is " + str(result) +" having Val "+ str(cur) +" and Severity : "+ str(current_severity))
						
					else:
						continue
		elif host_state=="down" and ds == "pl":
			return host_state
		else:
			return "up"

		if (ds == "pl" or ds == "rta"):
			return 'up'
		else:
			return "ok"
		#only required for UP and Down servereties of network devices

	def age_since_last_state(host, service, state, memc_conn):

		prefix = 'util:'
		key = prefix + host + ':' + service
		out = memc_conn.get(str(key))
		set_key = True
		timestamp = datetime.now() 
		now = (timestamp + timedelta(minutes=-(timestamp.minute % 5))).replace(second=0, microsecond=0)
		now = now.strftime('%s')
		age = now
		value = state + ',' + age
		if out:
			out = out.split(',')
			old_state = out[0]
			time_since = out[1]
			if old_state == state:
				set_key = False
				age = time_since
		if set_key:
			memc_conn.set(str(key), value)
			return int(age)

	def get_severity_values(service):
		global rules
		all_sev = rules.get(service)
		sev_values = []
		for i in range(1,len(all_sev)+1):
			sev_values.append(all_sev.get("Severity"+str(i))[1].get("value"))
		return sev_values

    	def extract_utilization_kpi(**kwargs):

		params = kwargs.get('params')

		technology = params.get('technology')
		devices = params.get('devices')
		memc_conn = params.get('memc_conn')
		redis_conn = params.get('redis_conn')
    		site_name = params.get('site_name')
    		machine_name = params.get('machine_name')
    		attributes = params.get('attributes')
    		slot_number = params.get('slot_number')

		service_name = attributes.get('service_name')
    		utilization_type = attributes.get('utilization_type')
    		utilization_key = attributes.get('utilization_key')
    		sector_type = attributes.get('sector_type')
		utilization_kpi_capacity = attributes.get('capacity')

		extracted_data_key = str("extracted__utilization__%s__%s_%s__%s__%s"%(technology,machine_name,site_name,service_name,slot_number))
		extracted_data = []

		for each_device in devices:

			data_dict = dict()
			sector_id_suffix = "_%s_sec" % sector_type
	    		bw_suffix = "_%s_bw" % sector_type

            		sector_id = memc_conn.get( "".join([str(each_device.get('hostname')) ,str(sector_id_suffix)]))
            		utilization = memc_conn.get(utilization_key%(each_device.get('hostname')))
            		sector_bw = memc_conn.get("".join([str(each_device.get('hostname')) ,str(bw_suffix)]))

            		if sector_bw and isinstance(sector_bw,basestring):
       	        		sector_bw = literal_eval(sector_bw)
       	    		if utilization and isinstance(utilization,basestring):
       	        		utilization = literal_eval(utilization)

       	        	data_dict['sector_id'] = sector_id
       	        	data_dict['utilization'] = utilization
       	        	data_dict['sector_bw'] = sector_bw

       	        	extracted_data.append(data_dict)

		redis_conn.rpush(extracted_data_key, extracted_data)

    	def transform_utilization_kpi(**kwargs):

		params = kwargs.get('params')

		technology = params.get('technology')
		devices = params.get('devices')
		memc_conn = params.get('memc_conn')
		redis_conn = params.get('redis_conn')
    		site_name = params.get('site_name')
    		machine_name = params.get('machine_name')
    		attributes = params.get('attributes')

		service_name = attributes.get('service_name')
    		utilization_type = attributes.get('utilization_type')
    		utilization_key = attributes.get('utilization_key')
    		sector_type = attributes.get('sector_type')
		utilization_kpi_capacity = attributes.get('capacity')

		severity_values = get_severity_values(service_name)
		critical_severity, warning_severity = severity_values

		transformed_data = []

		transformed_data_key = str("transformed__utilization__%s__%s__%s__%s__%s"%(technology,machine_name,site_name,service_name,slot_number))
		extracted_data_key = str("extracted__utilization__%s__%s__%s__%s__%s"%(technology,machine_name,site_name,service_name,slot_number))
		extracted_data = redis_conn.rget(extracted_data_key)

		for each_dict in extracted_data:

			utilization = each_dict.get('utilization')
			sector_bw = each_dict.get('sector_bw')
			sector_id = each_dict.get('sector_id')

			perf = ""
			utilization_kpi = ""			
			data_dict = {}
			state_string = "unknown"

			try:
	    			if utilization:
	    				if technology == "StarmaxIDU" and sector_bw:

		    				if sector_bw <= 3:
		    					utilization_kpi = (float(utilization)/int(utilization_kpi_capacity))*100
		    				elif sector_bw  > 3:
		    					utilization_kpi = (float(utilization)/(2*int(utilization_kpi_capacity)))*100
		    				else:
		    					utilization_kpi = ''

		    			if technology == "CanopyPM100AP":

		    				utilization_kpi = (float(utilization)/int(utilization_kpi_capacity))*100
		
	    			if isinstance(utilization_kpi,(float,int)):

					if technology == "StarmaxIDU":
		    				if utilization_kpi > 100:
		    					utilization_kpi = 100.00

		    			state_string = calculate_severity(service, utilization_kpi)


				perf += '%s_%s_util_kpi' %(sector_type,utilization_type) + "=%s;%s;%s;%s" % (utilization_kpi, warning_severity, critical_severity, sector_id)
				perf = 'cam_%s_util_kpi' % util_type + "=%s;%s;%s;%s" %(cam_util,args['war'],args['crit'],sec_id)

	    		except Exception as e:
	        		perf = ';%s;%s' % (warning_severity, critical_severity)
	        		perf = 'cam_%s_util_kpi' % util_type + "=;%s;%s;%s" %(args['war'],args['crit'],sec_id)

	        		logging.error('Exception: {0}\n'.format(e))

	    		age_of_state = age_since_last_state(each_device.get('hostname'), service, state_string, memc_conn)

			data_dict['host_name'] = each_device.get('hostname')
			data_dict['address'] = each_device.get('ip_address')
			data_dict['site'] = site_name
			data_dict['perf_data'] = perf
			data_dict['last_state_change'] = age_of_state
			data_dict['state']  = state_string
			data_dict['last_chk'] = time.time()
			data_dict['service_description'] = service
			data_dict['age']= age_of_state
	    		data_dict['refer'] = sector_id

	    		transformed_data.append(data_dict)

		redis_conn.rpush(transformed_data_key, trasformed_data)

    	def aggregate_utilization_kpi(**kwargs):

		params = kwargs.get('params')

		technology = params.get('technology')
		machine_name = params.get('machine_name')
		redis_conn = params.get('redis_conn')

		transformed_data_key_pattern = str("transformed__utilization__%s__%s*"%(technology,machine_name)) 
		aggregated_data_key = str("aggregated__utilization__%s__%s"%(technology, machine_name))
		aggregated_data = []

		transformed_data_keys = redis_conn.keys(transformed_data_key_pattern)
		for each_key in transformed_data_keys:
			transformed_data = redis_conn.rget(each_key)
			aggregated_data.extend(transformed_data)

		redis_conn.rpush(aggregated_data_key, aggregated_data)
    	
    	aggregate_utilization_kpi_dependency = dict()
    	for each_machine_name in machine_names:
	
		aggregate_utilization_kpi_task = PythonOperator(
		    	task_id = "Aggregate_utilization_kpi_of_%s"%each_machine_name,
		    	provide_context = True,
		    	python_callable = aggregate_utilization_kpi,
		    	params = {
		    		'technology': technology,
		    		"machine_name": each_machine_name,
		    		"redis_conn": redis_cnx
		    		},
			dag=utilization_kpi_by_technology
			)

		aggregate_utilization_kpi_dependency[each_machine_name] = aggregate_utilization_kpi_task

		"""
		insert_data_in_mysql_task = DummyOperator(
			task_id ="Insert_into_mysql_of_%s"%each_machine_name,
			dag=utilization_kpi_by_technology
			)

		update_data_in_mysql_task = DummyOperator(
			task_id ="Update_into_mysql_of_%s"%each_machine_name,
			dag=utilization_kpi_by_technology
			)

		update_data_in_mysql_task << aggregate_utilization_kpi_task
		insert_data_in_mysql_task << aggregate_utilization_kpi_task
   		"""

    	for each_site_name in site_names:
		machine_name = each_site_name.split("_")[0]
    		devices_by_site = devices.get(each_site_name)

    		for each_attribute in attributes:
    	    		slot_number = 1    	

    	   		while devices_by_site:
	        		slot_of_devices = devices_by_site[:100]

				if slot_of_devices:
    	            			extract_utilization_kpi_task = PythonOperator(
						task_id = "Extract_of_%s_%s_Slot_%s"%(each_site_name, each_attribute.get('service_name'), slot_number),
						provide_context = True,
						python_callable = extract_utilization_kpi,
						params = { 
							'technology': technology,
							"site_name": each_site_name,
				   			"machine_name": machine_name,
				   			"devices": slot_of_devices,
				   			"attributes": each_attribute,
				   			"memc_conn": memc_cnx,
				   			"redis_conn": redis_cnx,
							"slot_number": slot_number
				 		},
						dag = utilization_kpi_by_technology
					)
		    			
 	    	    			transform_utilization_kpi_task = PythonOperator(
  	    					task_id = "Transform_of_%s_%s_Slot_%s"%(each_site_name, each_attribute.get('service_name'), slot_number),
						provide_context = True,
						python_callable = transform_utilization_kpi,
						params = { 
							'technology': technology,
							"site_name": each_site_name,
							"machine_name": machine_name,
				    			"devices": slot_of_devices,
				    			"attributes": each_attribute,
				    			"memc_conn": memc_cnx,
				    			"redis_conn": redis_cnx,
				    			"slot_number": slot_number
				 		},
						dag = utilization_kpi_by_technology
	    				)
		    
                    			extract_utilization_kpi_task >> transform_utilization_kpi_task
		    
	            			aggregate_utilization_kpi_dependency[machine_name] << transform_utilization_kpi_task
	            			
	        		devices_by_site = devices_by_site[100:]
	        		slot_number += 1

    	return utilization_kpi_by_technology

