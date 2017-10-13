###############Utility functions for PRovision################
import re
from airflow.models import Variable
from airflow.hooks import RedisHook
import time
import logging
import traceback
from airflow.hooks import  MemcacheHook
import math
import time



redis_hook_2 = RedisHook(redis_conn_id="redis_hook_2") #DO NOT FLUSH THIS  DB !!!
redis_hook_7 = RedisHook(redis_conn_id="redis_hook_7")

rules = eval(Variable.get('rules'))
util_tech = eval(Variable.get('utilization_kpi_technologies'))
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
operators = eval(Variable.get('operators'))
service_state_type = ['warning','critical']
all_device_type_age_dict ={}
backhaul_inventory = {}
basestation_inventory = {}
for device in util_tech:
	try:
		all_device_type_age_dict[device] = eval(redis_hook_2.get("kpi_util_prev_state_%s"%(device)))
	except Exception:
		logging.error("No Prev states found")
		all_device_type_age_dict[device] = {}
backhaul_inventory = eval(redis_hook_2.get("backhaul_capacities"))
basestation_inventory = eval(redis_hook_2.get("basestation_capacities"))

#############################################SS UL ISSUE###############################################################################################
def calculate_wimax_utilization(wimax_util_data,wimax_pmp_bandwidth,capacity):



	if wimax_pmp_bandwidth and isinstance(wimax_pmp_bandwidth,basestring):
		wimax_pmp_bandwidth = eval(wimax_pmp_bandwidth)
	if wimax_util_data and isinstance(wimax_util_data,basestring):
		wimax_util_data = eval(wimax_util_data)


  	try:
		if wimax_util_data == 0.00:
			return 0.00
		if wimax_pmp_bandwidth and wimax_util_data and capacity:
			if wimax_pmp_bandwidth <= 3:
				sec_kpi = (float(wimax_util_data)/int(capacity)) *100
			elif wimax_pmp_bandwidth  > 3:
				sec_kpi = (float(wimax_util_data)/(2*int(capacity))) *100
			else:
				sec_kpi = ''
		if isinstance(sec_kpi,(float,int)) :
			if sec_kpi > 100:
				sec_kpi = 100.00


		return round(sec_kpi,2)
	except Exception,e:

		logging.warning("Unable to calculate KPI for %s"%(e))





def calculate_cambium_ss_utilization(cambium_ul_rssi,cambium_dl_rssi,cambium_dl_jitter,cambium_ul_jitter,cambium_rereg_count):
	ss_state = ""
	try:
		if cambium_ul_rssi != None and cambium_dl_rssi != None and (int(cambium_ul_rssi) < -82 or int(cambium_dl_rssi) < -82):
			ss_state = "los"
			state = 0
			state_string= "ok"
		elif cambium_dl_jitter !=None and cambium_ul_jitter != None and ( int(cambium_ul_jitter) > 7 or int(cambium_dl_jitter) > 7):
			ss_state = "jitter"
			state = 0
			state_string= "ok"
		elif cambium_rereg_count != None and (int (cambium_rereg_count) > 100):
			ss_state = "rereg"
			state = 0
			state_string = "ok"
		else:
			ss_state = "normal" 
			state_string= "ok"
			state = 0

		return ss_state
	except Exception:
		logging.error("Error in Calculating Cambium SS provisioning KPI")
		return ss_state

def calculate_radwin5k_ss_utilization(utilization):

	if utilization != None:
		utilization_kpi = (float(utilization)/10)
		utilization_kpi = round(utilization_kpi,2)
		
		return utilization_kpi

	else:
		return None

def calculate_radwin5k_bs_utilization(utilization,channel_bandwidth,util_type):
	if channel_bandwidth != None:
		if int(channel_bandwidth) == 10:
			ul_capacity = 5
			dl_capacity = 10 
		elif int(channel_bandwidth) == 5:
			ul_capacity = 5
			dl_capacity = 3
	else:
		return None

	if utilization != None:
		if util_type == 'ul':
			utilization_kpi = (float(utilization)/ul_capacity)*100
		elif util_type == 'dl':
			utilization_kpi = (float(utilization)/dl_capacity)*100

		utilization_kpi = round(utilization_kpi,2)
		
		return utilization_kpi

	else:
		return None

def calculate_radwin5kjet_ss_utilization(utilization):

	if utilization != None:
		utilization_kpi = (float(utilization)/10)
		utilization_kpi = round(utilization_kpi,2)
		
		return utilization_kpi

	else:
		return None

def calculate_radwin5kjet_bs_utilization(utilization,channel_bandwidth,util_type):
	
	if int(channel_bandwidth) == 10:
		ul_capacity = 5
		dl_capacity = 10 
	elif int(channel_bandwidth) == 5:
		ul_capacity = 5
		dl_capacity = 3


	if utilization != None:
		if util_type == 'ul':
			utilization_kpi = (float(utilization)/ul_capacity)*100
		elif util_type == 'dl':
			utilization_kpi = (float(utilization)/dl_capacity)*100

		utilization_kpi = round(utilization_kpi,2)
		
		return utilization_kpi

	else:
		return None

	



def calculate_radwin5k_bs_and_ss_dyn_tl_kpi(utilization):
	if utilization != None:
		utilization = round(utilization,2)
		utilization_kpi = (utilization*63)/100  # Dyncamic TL
		utilization_kpi = round(utilization_kpi,2)
	else:
		return None

def calculate_backhaul_utilization(utilization,hostname): #hostname is required for backhaul devices so as to get capacity for the devices
	#capacity=backhaul_inventory.get(hostname).get('port_wise_capacity')
	device_port = backhaul_inventory.get(hostname)
	capacity = device_port.get("capacity")
	if utilization != None:
		print utilization
		utilization = dict(utilization)
		all_ports_kpi_utilization = {}
		for port_name in utilization.keys():
			data_source = port_name+"_kpi"
			current_util = utilization.get(port_name)						 
			try:
				utilization_kpi = (float(current_util)/float(capacity.get(port_name))) *100
				utilization_kpi = round(utilization_kpi,2)
				all_ports_kpi_utilization.update({data_source:utilization_kpi})
			except Exception,e:
				all_ports_kpi_utilization.update({data_source:None})
				continue
		
		return all_ports_kpi_utilization
	else:
		return None

def calculate_mrotek_utilization(utilization,hostname): #hostname is required for backhaul devices so as to get capacity for the devices
	#capacity=backhaul_inventory.get(hostname).get('port_wise_capacity')
	device_port = backhaul_inventory.get(hostname)
	capacity = device_port.get("capacity")
	if utilization != None:
		utilization = utilization.split(',')
		port_name = "fe_"+utilization[1]
		all_ports_kpi_utilization = {}
		data_source = port_name+"_kpi"
		current_util = 	utilization[0]					 
		try:
			utilization_kpi = (float(current_util)/float(capacity.get(port_name))) *100
			utilization_kpi = round(utilization_kpi,2)
			all_ports_kpi_utilization.update({data_source:utilization_kpi})
		except Exception,e:
			all_ports_kpi_utilization.update({data_source:None})
			
		
		return all_ports_kpi_utilization
	else:
		return None



def calculate_ptp_utilization(utilization,hostname):
	if utilization and utilization != None and hostname:
		qos_bandwidth = basestation_inventory.get(hostname).get('qos_bandwidth')
		if qos_bandwidth != None:
			utilization_kpi = (float(utilization)/float(qos_bandwidth/1024.0)) * 100
			utilization_kpi = round(utilization_kpi,2)
			return utilization_kpi

	else:
		return None


		##########################################################################HELPERS##########################################



		# 								  _______  __	   _______  _______  _______  _______
		#						|\	 /| (  ____ \( \	  (  ____ )(  ____ \(  ____ )(  ____ \									#
		#						| )   ( || (	\/| (	  | (	)|| (	\/| (	)|| (	\/										#		
		#						| (___) || (__	| |	  | (____)|| (__	| (____)|| (_____										# 
		#						|  ___  ||  __)   | |	  |  _____)|  __)   |	 __)(_____  )
		#						| (   ) || (	  | |	  | (	  | (	  | (\ (		 ) |
		#						| )   ( || (____/\| (____/\| )	  | (____/\| ) \ \__/\____) |
		#						|/	 \|(_______/(_______/|/	   (_______/|/   \__/\_______



		############################################################################################################################

		
def get_severity_values(service):
	all_sev = rules.get(service)
	sev_values = []
	for i in range(1,len(all_sev)+1):
		sev_values.append(all_sev.get("Severity"+str(i))[1].get("value"))
	return sev_values

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
			

"""
This function is used to calculate age for the give severity and add it in the dict
"""

def calculate_age(hostname,current_severity,device_type,current_time,service_name):

	try:

		previous_device_state = all_device_type_age_dict.get(device_type)
		device_state = previous_device_state.get(hostname+"_"+service_name)
		if device_state.get("state") == current_severity:
			age = device_state.get("since")
			return age

			if age != None and age != '':
				if age == 'None':
					return current_time
				else:
					return age
			else:
				logging.error("Got the devices %s but unable to fetch the since key ,its either None or Empty"%(hostname))
				return 0

		elif device_state.get("state") != current_severity:
			return current_time
			
	except AttributeError:
		logging.error("Device with the provided key not found ")
		return 0
		
	except Exception:
		logging.error("Unable to get state for device %s will be created when updating refer"%(hostname))
		traceback.print_exc()
		return 0
		#traceback.print_exc()


def backtrack_x_min(x,seconds):
	value= int(math.floor(x / seconds)) * seconds
	return value
