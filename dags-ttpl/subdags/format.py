from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from subdags.format_utility import get_threshold
from subdags.format_utility import get_device_type_from_name
from subdags.format_utility import get_previous_device_states
from subdags.format_utility import memcachelist
from subdags.format_utility import forward_five_min
from subdags.format_utility import backtrack_x_min
from subdags.events_utility import get_device_alarm_tuple
from subdags.events_utility import update_device_state_values
from subdags.events_utility import update_last_device_down
from airflow.operators import ExternalTaskSensor
import json
import logging
import traceback
from airflow.hooks import  MemcacheHook
import time
import math
import sys
#TODO: Create operator changed from previous 

default_args = {
	'owner': 'wireless',
	'depends_on_past': False,
	'start_date': datetime.now() - timedelta(minutes=2),
	'email': ['vipulsharma144@gmail.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=1),
	'provide_context': True,
	'catchup': False,
	# 'queue': 'bash_queue',
	# 'pool': 'backfill',
	# 'priority_weight': 10,
	# 'end_date': datetime(2016, 1, 1),
}

##############################DAG CONFIG ENDS###############################################33333################################
aggregate_nw_tasks={}
aggregate_sv_tasks={}
logging.basicConfig(level=logging.ERROR)
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
rules = eval(Variable.get('rules'))
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
exclude_network_datasource = eval(Variable.get("exclude_network_datasource"))
databases=eval(Variable.get('databases'))
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
redis_hook_2 = RedisHook(redis_conn_id="redis_cnx_2")
redis_availablity_0 = RedisHook(redis_conn_id="redis_availablity_0")
all_devices_states = get_previous_device_states(redis_hook_5)
all_devices_states_rta = get_previous_device_states(redis_hook_5,"rta")
redis_hook_network_alarms = RedisHook(redis_conn_id="redis_hook_network_alarms")
event_rules = eval(Variable.get('event_rules'))
operators = eval(Variable.get('operators')) #get operator Dict from 
config = eval(Variable.get("system_config"))
debug_mode = eval(Variable.get("debug_mode"))
activate_all_tab = eval(Variable.get("activate_all_tab"))
result_nw_memc_key = []
result_sv_memc_key = []
HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'


#################################Init Global Var ends###################################################################################

def format_etl(parent_dag_name, child_dag_name, start_date, schedule_interval, celery_queue):
	network_slots = list(redis_hook_4.get_keys("nw_ospf*"))
	service_slots = list(redis_hook_4.get_keys("sv_ospf*"))
	network_slots.extend(redis_hook_4.get_keys("nw_vrfprv*")) #TODO: very bad approach to get pub and vrf daata
	service_slots.extend(redis_hook_4.get_keys("sv_vrfprv*"))
	network_slots.extend(redis_hook_4.get_keys("nw_pub*")) #TODO: very bad approach to get pub and vrf daata
	service_slots.extend(redis_hook_4.get_keys("sv_pub*"))
	logging.info("****Calling Format ETL Subdag****")
	temp_dir_path = ""
	
	dag_subdag_format = DAG(
			dag_id="%s.%s" % (parent_dag_name, child_dag_name),
			schedule_interval=schedule_interval,
			start_date=start_date,
 		)
	def get_severity_values(service):

		all_sev = rules.get(service)
		
		try:
			if all_sev:
				severity_len = len(all_sev)+1
			else:
				severity_len = 0
				logging.warning("No rules in rules varaible for %s"%(service))
		except Exception:
			logging.error("No rules in rules varaible for %s due which exception is generated."%(service))


		sev_values = []
		for i in range(1,severity_len):
			sev_values.append(all_sev.get("Severity"+str(i))[1].get("value"))
		return sev_values
	#TODO: Add EXOR iff required

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
				logging.info("The specified service "+service+" does not have a rule specified in rules variable")
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

	def calculate_refer(hostname,current_sev,ds,all_down_devices_states,processing_time):
		
		if current_sev == "down" or current_sev == "up":
			try:
				if all_down_devices_states.get(hostname).get('state') == None:
					logging.info("Got None")
					return 0
				if all_down_devices_states.get(hostname).get('state') != current_sev: # to see if severity has chnaged
					old_sev = all_down_devices_states.get(hostname).get('state') 
					if current_sev == "down" and old_sev == "up":
						return processing_time #backtrack 5	 min slot to give last down time
					elif current_sev == "up" and old_sev == "down":
						#return backtrack_x_min(processing_time,300)
						return all_down_devices_states.get(hostname).get('since')
						#return forward_five_min(int(time.time())) #Formawrd 5 min slot
				elif all_down_devices_states.get(hostname).get('state') == current_sev:
					#TODO : See this please something is wrong
					
					if all_down_devices_states.get(hostname).get('since') != 'None':
						since_time =  all_down_devices_states.get(hostname).get('since')
						#logging.info("Since %s %s"%(since_time,hostname))
						return since_time

			except AttributeError:
				
				logging.info("Problem in fetching the refer value .Not able to find the host %s "%hostname)
				
				#traceback.print_exc()
				
			except Exception:
				logging.info("Error while calculating refer")
				
				
		else:
			try:
				#logging.info("HOST : %s %s"%(len(all_down_devices_states),hostname))
				if all_down_devices_states.get(hostname):
					since_time =  all_down_devices_states.get(hostname).get('since')
					#logging.info("Returning  %s "%(since_time))
					return since_time
			except Exception:
				logging.info("Device %s not found in the dict for ds : %s"%(hostname,ds))
				
				

	"""
	This function is used to calculate age for the give severity and add it in the dict
	"""

	def calculate_age(hostname,current_severity,datasource,current_time):
		if datasource == "pl":
			previous_device_state = all_devices_states
		elif datasource == "rta":
			previous_device_state = all_devices_states_rta
		else:
			logging.info("Datasource is neither rta nor pl")
			return 0

		try:
			device_state = previous_device_state.get(hostname)
			if device_state.get("state") == current_severity:
				age = device_state.get("since")
				return age

				if age != None and age != '':
					if age == 'None':
						return current_time
					else:
						return age
				else:
					logging.info("Got the devices %s but unable to fetch the since key "%(hostname))
					return 0

			elif device_state.get("state") != current_severity:
				return current_time
				

		except Exception:
			logging.info("Unable to get state for device %s will be created when updating refer"%(hostname))
			traceback.print_exc()
		

###########################################################------------NETWORK--------------- ##################################################
	def network_format(**kwargs):
		
		device_to_be_converted_down =eval(Variable.get("device_converted_down"))
		redis_queue_slot=kwargs.get('task_instance_key_str').split("_")[2:-3]
		all_down_devices_states = get_previous_device_states(redis_hook_5,"down")	

		state_has_changed = True #TODO: this variable is used to decided that do 
								 #we need to update teh refer dict in memc and redis \
								 #i.e it is only changed if there is some updation in the dict , currently defaulted to dict 
		logging.info("Getting from redis Key ->"+ "_".join(redis_queue_slot))
		redis_queue_slot="_".join(redis_queue_slot)
		slot_data = redis_hook_4.rget(redis_queue_slot)
		if len(slot_data) <= 0 :
			return "No Data"

		#slot_data = [[u'110556',u'10.171.132.2',0,1491822300,1491622173,u'rta=1.151ms;50.000;60.00;0;pl=30%;10;20;; rtmax=1.389ms;;;; rtmin=1.035ms;;;;']] * 10
		network_list = []
		for slot in slot_data:
			slot=eval(slot)
			network_dict = {
				'site': 'unknown' ,
				'host': 'unknown',
				'service': 'unknown',
				'ip_address': 'unknown',
				'severity': 'unknown',
				'age': 'unknown',
				'ds': 'unknown',
				'cur': 'unknown',
				'war': 'unknown',
				'cric': 'unknown',
				'check_time': 'unknown',
				'local_timestamp': 'unknown' ,
				'refer':'unknown',
				'min_value':'unknown',
				'max_value':'unknown',
				'avg_value':'unknown',
				'machine_name':'unknown'
				}
			try:
				#logging.info("Getting From %s"%slot[0])
				device_type = get_device_type_from_name(slot[0]) #HANDLE IF DEVICE NOT FOUND
			except ValueError:
				logging.error("Couldn't find Hostmk dict need to calculate the thresholds Please run SYNC DAG")
				#TODO: Automatically run sync dag here if not found
			except Exception:
				logging.error("Unable to find the deivce tpe for %s"%slot[0])
				traceback.print_exc()
			#print "len of slot is ==========> "+str(len(slot))
			

			threshold_values = get_threshold(slot[-1])

			rt_min_cur = threshold_values.get('rtmin').get('cur')
			rt_max_cur = threshold_values.get('rtmax').get('cur')
			host_state = "up" if not int(slot[2]) else "down"
			network_dict['site'] = "_".join(redis_queue_slot.split("_")[1:4])
			network_dict['host'] = slot[0]
			network_dict['service'] ="ping"
			network_dict['ip_address'] = slot[1]			
			network_dict['check_time'] = int((slot[3]))if slot[3] else 0
			network_dict['local_timestamp'] = forward_five_min(int(slot[3])) #cieled in next  multiple of 5
			network_dict['min_value'] = rt_min_cur
			network_dict['max_value'] = rt_max_cur
			network_dict['avg_value'] = round((float(rt_max_cur)+float(rt_min_cur))/2,2)
			network_dict['machine_name'] =  redis_queue_slot.split("_")[1]
			#print slot[0]
			#print device_type
			

			for data_source in threshold_values:
				

				if data_source in exclude_network_datasource:
					continue
				

				value = threshold_values.get(data_source).get("cur")
				key=str(device_type+"_"+data_source)
				thresholds_from_rules = get_severity_values(key)
				
				if float(value) == 100:
					host_state = "down"
				else:
					host_state = "up"

				if network_dict['ip_address'] in device_to_be_converted_down:
					network_dict['severity'] ="down"
				else:
					#logging.info("IP: %s"%(slot[1]))	
					network_dict['severity'] =calculate_severity(key,value,host_state,data_source)
					
				network_dict['ds'] = data_source
				network_dict['cur'] = value
				network_dict['war'] = thresholds_from_rules[1] if thresholds_from_rules else ''
				network_dict['cric'] = thresholds_from_rules[0] if thresholds_from_rules else ''
				network_dict['refer'] = str(calculate_refer(slot[0],network_dict['severity'],data_source,all_down_devices_states,network_dict['local_timestamp'])) #TODO: Here the same refer is beig calculate for pl and rta which will although be one only
				network_dict['age'] = calculate_age(slot[0],network_dict['severity'],data_source,network_dict['local_timestamp'])

				network_list.append(network_dict.copy())

		try:		
			#create_file_and_write_data(redis_queue_slot+"_result",str(network_list)) #TODO: Here file is overwritten and no record is maintened it is to be discussed
			if len(network_list) > 0:
				redis_hook_4.rpush(str(redis_queue_slot)+"_result",network_list)
				logging.info("Redis Connection made and data inserted successfully")
				logging.info("Successfully Inserted data to Redis KEY :"+redis_queue_slot+"_result")
			else:
				logging.info("No Data At %s"%(str(redis_queue_slot)))
				redis_hook_4.rpush(str(redis_queue_slot)+"_result",[])
		except Exception:
			logging.info("Unable to Insert to Redis")
			traceback.print_exc()
################################################################SERVICE############################################################
	def service_format(**kwargs):
		
		kpi_helper_services =['wimax_dl_intrf', 'wimax_ul_intrf', 'cambium_ul_jitter','cambium_rereg_count']
		rad5k_helper_service = ['rad5k_ss_dl_uas','rad5k_ss_ul_modulation']
		wimax_sector_id_list = ['wimax_pmp1_ul_util_bgp','wimax_pmp2_dl_util_bgp','wimax_pmp1_dl_util_bgp','wimax_pmp2_ul_util_bgp']
		provis_services= ['wimax_ul_rssi','wimax_dl_rssi','wimax_dl_cinr','wimax_dl_cinr','wimax_ss_ptx_invent','cambium_ul_rssi','cambium_dl_rssi','cambium_dl_jitter','cambium_ul_jitter','cambium_rereg_count','radwin_rssi','radwin_uas']
		rad5k_jet_helper_service = ['rad5kjet_ss_uas','rad5kjet_ss_ul_modulation']
		ss_provis_helper_serv_data = []
		data_dict_sample =  {'age': 'unknown',
		 'check_time': 'unknown',
		 'ds': 'unknown',
		 'host': 'unknown',
		 'ip_address': 'unknown',
		 'local_timestamp': 'unknown',
		 'cric': 'unknown', 
		 'cur': 'unknown',
		 'war':'unknown',
		 'refer': 'unknown',
		 'service': 'unknown',
		 'severity':'unknown',
		 'site': 'unknown',
		 'machine_name':'unknown'}
		redis_queue_slot=kwargs.get('task_instance_key_str').split("_")[2:-3]
		redis_queue_slot="_".join(redis_queue_slot)
		logging.info("Getting from redis Key ->"+ redis_queue_slot)
		site_name = "_".join(redis_queue_slot.split("_")[1:4])
		device_down_list = redis_hook_4.rget("current_down_devices_%s"%site_name)
		start_time = time.time()
		slot_data = redis_hook_4.rget(redis_queue_slot)
		logging.info("Time for redis Input = "+ str(time.time() - start_time))
		service_list = []
		start_time = time.time()

		try:
			if len(slot_data) > 0:
				for device_data in slot_data:
					
					data_dict = data_dict_sample 
					device_data = eval(device_data)
					refer = ""

					ds_values = device_data[7].split('=')
					

					if len(device_data) < 8 or device_data[0] in device_down_list or not len(device_data[-1]):
						logging.info("Ommiting device %s"%device_data[0])
						continue
	#################################################################ADDITIONAL HANDLING FOR PMP PORT##########################################
					if device_data[2] in wimax_sector_id_list:
							
							port_type = device_data[2].split("_")[1]
							key1 = str(device_data[0])+"_"+str(port_type)+"_sec"
							try:			  #str(hostname)+"_pmp1_sec"
								refer = memc_con.get(key1)
							except Exception:
								logging.info("Unable to find wimax sector data from MEMC")
								
							
					
	#####################################################################################################################################
					
					severity_war_cric  = get_severity_values(device_data[2])
					#logging.info("FOR device %s"%device_data[0])
					
					data_dict['host'] = device_data[0]
					data_dict['ip_address'] = device_data[1]
					data_dict['ds'] = ds_values[0] if len(ds_values) >= 1  else ''
					data_dict['check_time'] = int(device_data[4]) if device_data[4] else 0
					data_dict['local_timestamp'] = forward_five_min(int(device_data[4]))#Floored in previous  multiple of 5
					data_dict['service'] = device_data[2]
					data_dict['cur'] = ds_values[1].split(';')[0] if len(ds_values) > 1  else ''
					data_dict['war'] = severity_war_cric[1] if len(severity_war_cric) > 1 else ''
					data_dict['cric'] = severity_war_cric[0] if len(severity_war_cric) > 0 else ''
					#ds_values[1].split(';')[2]
					data_dict['severity'] =calculate_severity(device_data[2],ds_values[1].split(';')[0]) if len(ds_values) > 1  else '' #TODO: get data from static DB
					data_dict['age'] = int(device_data[5]) if device_data[5] else 0 #TODO: Calclate at my end change of severiaty
					data_dict['site'] = site_name
					data_dict['refer'] = refer
					data_dict['min_value'] = data_dict['cur']
					data_dict['max_value'] = data_dict['cur']
					data_dict['avg_value'] = data_dict['cur']
					data_dict['machine_name']=redis_queue_slot.split("_")[1]
					service_list.append(data_dict.copy())

	##########################SOME ADDITIONAL HANDLING OF SERVICES##########################################################################
					ip_address = data_dict['ip_address']

					if ip_address == '10.133.26.79':
						print data_dict
					
					value = data_dict['cur']
					if str(data_dict['service']) in rad5k_helper_service:
						
						if str(data_dict['service']) == 'rad5k_ss_ul_modulation':
								key = ip_address+ "_rad5k_ss_ul_mod"
								key = str(key)
								memcachelist(key,value,memc_con)
						elif str(data_dict['service']) == 'rad5k_ss_dl_uas':
								key = ip_address+ "_uas_list"
								key = str(key)
								memcachelist(key,value,memc_con)

					if str(data_dict['service']) in rad5k_jet_helper_service:
						
						if str(data_dict['service']) == 'rad5kjet_ss_ul_modulation':
								key = ip_address+ "_rad5kjet_ss_ul_mod"
								key = str(key)
								memcachelist(key,value,memc_con)
						elif str(data_dict['service']) == 'rad5kjet_ss_uas':
								key = ip_address+ "_rad5kjet_uas_list"
								key = str(key)
								memcachelist(key,value,memc_con)

					if str(data_dict['service']) in provis_services:
						ss_provis_helper_serv_data.append({
							'device_name': str(data_dict['host']),
							'service_name': str(data_dict['service']),
							'current_value': str(data_dict['cur'])
						})


					if str(data_dict['service']) in kpi_helper_services:
						key = ip_address+ "_"+str(data_dict['service'])
						key = str(key)
						logging.info("Setting %s"%(key))
						memcachelist(key,data_dict['severity'],memc_con)

					if device_data[2] in provis_services:
						key = "provis:"+str(device_data[0])+":"+str(device_data[2])
						#logging.info(key)
						try:
							memc_con.set(key,value)
						except Exception:
							logging.info("Unable to set Provisional KPI data into MEMC")

	####################################################################################################################ENDS##############################
				logging.info("Time for FOR LOOP  = "+ str(time.time() - start_time))
			else:
				logging.info("No Data for input")

		except IndexError,e:
			logging.info("Some Problem with the dataset ---> \n"+str(device_data))	
			
			traceback.print_exc()
		except Exception:
			traceback.print_exc()

		try:		
			#create_file_and_write_data(redis_queue_slot+"_result",str(service_list)) #TODO: Here file is overwritten and no record is maintened it is to be discussed
			json_obj = json.dumps(service_list)
			logging.info("About to dump data to Redis of size(JSON) of Service : KB -" + str(sys.getsizeof(json_obj)/1024 ))
			if len(json_obj) > 0 :
				redis_hook_4.rpush(str(redis_queue_slot)+"_result",service_list)			
				logging.info("Successfully Inserted data to redis KEY :"+redis_queue_slot+"_result of length "+str(len(service_list)))
			else:
				logging.info("0 Len data recieved after processing omitting ALL")
			if len(ss_provis_helper_serv_data) > 0:
				redis_hook_4.rpush("%s_provis_availablity"%(site_name),ss_provis_helper_serv_data)
			else:
				logging.info("No Provis helper data")
		except Exception:
			logging.info("Unable to Wite to redis key created")
			traceback.print_exc()


	#TODO: Break this task in two task nw and sv aggregate data
	def aggregate_nw_data(**kwargs):
		logging.info("Aggregating Network Data")
		nw_memc_keys = eval(Variable.get("network_memc_key"))
		task_for_machine=kwargs.get('task_instance_key_str').split("_")[3]
		global redis_hook_4
		nw_data = {}
		nw_data[str(task_for_machine)] = []	
		
		#Filter the data with None values i.e slot which are not processed or no data there to report
		for key in nw_memc_keys:
			if task_for_machine in key:
				redis_data_nw = redis_hook_4.rget(key)
				current_machine = key.split("_")[1]
				if redis_data_nw != None:
					nw_data.get(current_machine).append(redis_data_nw)
				else:
					logging.info("There is No Network data for slot in memc : "+key )
			else:
				continue
		
			#TODO: change the code so the setting of key for  both  become independent i.e set above
		try:
			logging.info("Here Dumping data the number should be 1 the actual number is %d"%len(nw_data))
			for nw_memc_keys in nw_data:
				logging.info("About to dump data to Memcache of size of Network : " + str(sys.getsizeof(nw_data)))
				json_obj = json.dumps(nw_data)
				logging.info("About to dump data to Memcache of size(JSON) of Service : " + str(sys.getsizeof(json_obj)))
				redis_hook_4.rpush("nw_agg_nocout_"+str(nw_memc_keys),nw_data.get(nw_memc_keys))
				memc_con.set("nw_agg_nocout_"+str(nw_memc_keys),nw_data.get(nw_memc_keys))
				logging.info("Dumped Network data to Memcache of length	" + str(len(nw_data)) +" at "+ "nw_nocout_"+str(nw_memc_keys))
			return True
		except Exception:
			logging.error("Unable to put in the combined Network data to memcache.")
			traceback.print_exc()
			return False


	def store_availablity_data_in_redis(**kwargs):
		nocout_site_name = kwargs.get('params').get('site')
		data_type = kwargs.get('params').get('dev_type')
		all_data = []

		this_time = datetime.now()
		t_stmp = this_time + timedelta(minutes=-(this_time.minute % 5))
		t_stmp = t_stmp.replace(second=0,microsecond=0)
		current_time =int(time.mktime(t_stmp.timetuple()))
		data = redis_hook_4.get("%s")
		try:
			if data_type == "network":
				set_name = nocout_site_name + "_network"
				keys = redis_hook_4.get_keys("nw_%s_*_result"%(nocout_site_name))
				
		   	else:
		   		set_name = nocout_site_name + "_service"
		   		keys = redis_hook_4.get_keys("sv_%s_*_result"%(nocout_site_name))

		   	for key in keys:
		   		
				data = redis_hook_4.rget(key)
				
				
				for datum in data:
					datum=eval(datum)
				all_data.extend(datum)
			if not debug_mode:
				try:
		   			redis_availablity_0.zadd_compress(set_name,current_time,all_data)
		   		except Exception:
		   			logging.error("Unable to add availablity data in redis check zadd_compress in redis_loader_hook")
		   	else:
		   		logging.info("Debug Mode is active  Not inserting availablity data")
		except Exception,e:
			logging.info("Error in storing redis : %s"%(e))
			pass


	def aggregate_sv_data(**kwargs):
		logging.info("Aggregating Service Data")
		sv_memc_keys = eval(Variable.get("service_memc_key"))
		
		task_for_machine=kwargs.get('task_instance_key_str').split("_")[3]

		sv_data = {}
		sv_data[str(task_for_machine)] = []


		#Filter the data with None values i.e slot which are not processed or no data there to report

		for key in sv_memc_keys:
			if task_for_machine in key:
				redis_data_sv = redis_hook_4.rget(key)
				current_machine = key.split("_")[1]
				if redis_data_sv != None:
					sv_data.get(current_machine).append(redis_data_sv)
				else:
					logging.warning("There is No Service data for slot in Redis : "+key )
				#TODO: change the code so the setting of key for  both  become independent i.e set above
		try:
			logging.info("Here Dumping data the number should be 1 the actual number is %d"%len(sv_data))
			for sv_memc_keys in sv_data:
				logging.info("About to dump data to Memcache of size of Service : " + str(sys.getsizeof(sv_data)))
				json_obj = json.dumps(sv_data)
				logging.info("About to dump data to Redis of size(JSON) of Service :(SIZE) " + str(sys.getsizeof(json_obj)) + " (NAME) sv_agg_nocout_"+str(sv_memc_keys))
				if sv_data.get(sv_memc_keys):
					redis_hook_4.rpush("sv_agg_nocout_"+str(sv_memc_keys),sv_data.get(sv_memc_keys))

					logging.info("Inserted data in redis")
					return True
				else:
					logging.info("Unable to insert SV ")
		except Exception:
			logging.error("Unable to put in the combined service data to memcache.")
			traceback.print_exc()
			return False
#this function is used to get all the slots and then combine their data into one site data
	def extract_and_distribute_nw(**kwargs):
	
		site=kwargs.get('params').get('site')
		all_pl_rta_trap_dict = {}
		all_pl_rta_trap_dict[site] = []
		start_time = time.time()
		for redis_key in network_slots:
			if "_result" in redis_key:
				continue

			if site in redis_key:
				try:
					network_data = redis_hook_4.rget(redis_key+"_result")

				except Exception:
					logging.error("Unable to get the result key from redis")
				if len(network_data) > 0:
					all_pl_rta_trap_dict.get(site).extend(get_device_alarm_tuple(network_data,event_rules))  #TODO: Imporove args
				else:
					logging.info("No Data Found in redis at key: %s"%(redis_key+"_result"))
		logging.info("TIME : %s"%(time.time() - start_time))
		start_time = time.time()
		
		if len(all_pl_rta_trap_dict.get(site)) > 0:
			redis_key = 'network_smptt_%s' % site
			try:
				redis_hook_4.rpush(redis_key,all_pl_rta_trap_dict.get(site))
				logging.info("successfully inserted data into key : %s"%(redis_key))
							
			except Exception:
				logging.error("Unable to insert data to redis.")
		else:
			logging.info("No Traps recieved")
		logging.info("TIME : %s"%(time.time() - start_time))	
		

	def aggregate_smptt(**kwargs):
		logging.info("Aggregating Network SMPTT Data")
		machine_data=[]
		task_for_machine=kwargs.get('params').get('machine')
		network_slots_smptt = redis_hook_4.get_keys("network_smptt_%s_*"%task_for_machine)
		for machine_slots_in_redis in network_slots_smptt:
			machine_data.extend(redis_hook_4.rget(machine_slots_in_redis))
		logging.info("--->%s"%len(machine_data))
		if len(machine_data) > 0:
			for k,traps in enumerate(machine_data):
				machine_data[k] = traps
			logging.info("%s of length %s"%("queue:network:snmptt:%s"%task_for_machine,len(machine_data)))
			if not debug_mode and activate_all_tab:
				redis_hook_network_alarms.rpush("queue:network:snmptt:%s"%task_for_machine,machine_data)
			else:
				logging.info("Debug Mode is active , not inserting into redis")
		else:
			logging.info("No Data Foubnd onto site : %s" %task_for_machine)


	
#########################################################################TASKS#######################################################################
	aggregate_nw_smptt_tasks = {}
	event_site_tasks = {}
	service_format_sensor_dict = {}
	network_format_sensor_dict = {}
	network_sensor_sites = []
	service_sensor_sites = []
	availablity_nw_tasks = {}
	availablity_sv_tasks = {}
	update_refer = PythonOperator(
							task_id="update_device_states",
							provide_context=False,
							python_callable=update_device_state_values,
							#params={"site":site},
							#redis_hook=redis_hook_4,

							dag=dag_subdag_format,
							queue=celery_queue
							)

	update_last_device_down_task = PythonOperator(
							task_id="update_last_device_down",
							provide_context=False,
							python_callable=update_last_device_down,
							#params={"site":site},
							#redis_hook=redis_hook_4,

							dag=dag_subdag_format,
							queue=celery_queue
							)
	for db in databases:
		db_name=db.split("_")[1]
		aggregate_sv_data_task = PythonOperator(
				task_id="aggregate_%s_sv_data"%db_name,
				provide_context=True,
				python_callable=aggregate_sv_data,
				#params={"ip":machine.get('ip'),"port":site.get('port')},
				dag=dag_subdag_format,
				trigger_rule = 'all_done',
				queue=celery_queue
				)
		aggregate_nw_data_task = PythonOperator(
				task_id="aggregate_%s_nw_data"%db_name,
				provide_context=True,
				python_callable=aggregate_nw_data,
				#params={"ip":machine.get('ip'),"port":site.get('port')},
				dag=dag_subdag_format,
				trigger_rule = 'all_done',
				queue=celery_queue,
				)
		aggregate_smptt_task =  PythonOperator(
				task_id="aggregate_%s_nw_smptt_data"%db_name,
				provide_context=True,
				python_callable=aggregate_smptt,
				params={"machine":db_name},
				dag=dag_subdag_format,
				trigger_rule = 'all_done',
				queue=celery_queue
				)
		aggregate_nw_tasks[db_name] = aggregate_nw_data_task
		aggregate_sv_tasks[db_name] = aggregate_sv_data_task
		aggregate_nw_smptt_tasks[db_name] = aggregate_smptt_task
		aggregate_smptt_task >> update_refer
		aggregate_smptt_task >> update_last_device_down_task
#####################################################################################################################################################
#####################################################################################################################################################

	try:
		result_nw_memc_key = []
		for machine in config:
			sites = machine.get('sites')
			for site in sites:
				site_name = site.get('name')
				total_slot = int(Variable.get("nw_%s_slots"%(site_name)))
				machine_name = site_name.split("_")[0] #To make UAT Compatible

				if site_name not in network_sensor_sites:
					nw_extract_task_sensor = ExternalTaskSensor( 
					external_dag_id="ETL.NETWORK",
					external_task_id="Network_extract_%s"%site_name,
					task_id="sense_nw_%s_extract_task"%site_name,
					poke_interval =2,
					trigger_rule = 'all_done',
					dag=dag_subdag_format,
					queue=celery_queue
					)
					network_sensor_sites.append(site_name)
					

				if site_name not in event_site_tasks.keys():
					event_nw = PythonOperator(
						task_id="discover_event_nw_%s_"%(site_name),
						provide_context=True,
						python_callable=extract_and_distribute_nw,
						params={"site":site_name},
						dag=dag_subdag_format,
						queue=celery_queue
						)
					event_site_tasks[site_name] = event_nw

					event_nw >> aggregate_nw_smptt_tasks.get(machine_name)

				if site_name not in availablity_nw_tasks.keys():
					availablity_task = PythonOperator(
						task_id="store_nw_%s"%(site_name),
						provide_context=True,
						python_callable=store_availablity_data_in_redis,
						params={"site":site_name,'dev_type':"network"},
						dag=dag_subdag_format,
						queue=celery_queue
						)
					availablity_nw_tasks[site_name] = availablity_task

				if site_name not in availablity_sv_tasks.keys():
					availablity_task = PythonOperator(
						task_id="store_sv_%s"%(site_name),
						provide_context=True,
						python_callable=store_availablity_data_in_redis,
						params={"site":site_name,'dev_type':"service"},
						dag=dag_subdag_format,
						queue=celery_queue
						)
					availablity_sv_tasks[site_name] = availablity_task
					



				for slot in range(1,total_slot+1):
					network_tasks = None
					task = ("nw_%s_slot_%s"%(site_name,slot))
					task_name = task.split("_")
					result_list = task.split("_")


					task_name.append("format")
					result_list.append("result")

					name = "_".join(task_name)
					result_nw_memc_key.append("_".join(result_list))

					network_tasks=PythonOperator(
						task_id="%s"%name,
						provide_context=True,
						python_callable=network_format,
						#params={"previous_all_device_states":previous_all_device_states},
						dag=dag_subdag_format,
						queue=celery_queue

						)

					try:
						nw_extract_task_sensor >> network_tasks
						
						network_tasks >> event_site_tasks.get(site_name)
						network_tasks >> availablity_nw_tasks.get(site_name)
						network_tasks >> aggregate_nw_tasks.get(machine_name)
					except Exception:
						logging.info("Unable to attach tasks to %s"%site)
						traceback.print_exc()
			
		Variable.set("network_memc_key",str(result_nw_memc_key))
	except Exception:
		logging.error("There is an error while create format tasks for network data")
		traceback.print_exc()

#####################################################################################################################################################
#####################################################################################################################################################
	try:
		result_sv_memc_key=[]
		for machine in config:
			sites = machine.get('sites')		

			for site in sites:
				site_name = site.get('name')
				total_slot = int(Variable.get("sv_%s_slots"%(site_name)))
				#machine_name = machine.get("Name")
				machine_name = site_name.split("_")[0]
				if site_name not in service_sensor_sites:
					service_format_task_sensor =  ExternalTaskSensor( #here task for the same site could be made more than once but it get overriden at the end
					external_dag_id="ETL.SERVICE",
					external_task_id="Service_extract_%s"%site_name,
					task_id="sense_sv_%s_extract_task"%site_name,
					poke_interval =2,
					trigger_rule = 'all_done',
					#sla=timedelta(minutes=1),
					dag=dag_subdag_format,
					queue=celery_queue
					)
				   	service_sensor_sites.append(site_name)

				for slot in range(1,total_slot+1):

					task = ("sv_%s_slot_%s"%(site_name,slot))
					task_name = task.split("_")
					result_list = task.split("_")

					task_name.append("format")
					result_list.append("result")

					result_sv_memc_key.append("_".join(result_list))

					name = "_".join(task_name)
					service_tasks = PythonOperator(
						task_id="%s"%name,
						provide_context=True,
						python_callable=service_format,
						#params={"ip":machine.get('ip'),"port":site.get('port')},
						dag=dag_subdag_format,
						queue=celery_queue
						)
			
					service_format_task_sensor >> service_tasks
					service_tasks >> aggregate_sv_tasks.get(machine_name)
					service_tasks >> availablity_sv_tasks.get(site_name)
		
		Variable.set("service_memc_key",str(result_sv_memc_key))
	except Exception:
		logging.error("There is an error while create format tasks for Service data")
		traceback.print_exc()

	
	return 	dag_subdag_format
