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
ul_tech = eval(Variable.get('provision_kpi_technologies'))
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
operators = eval(Variable.get('operators'))
service_state_type = ['warning','critical']
all_device_type_age_dict ={}

for techs_bs in ul_tech:
	techs_ss = ul_tech.get(techs_bs)
	all_device_type_age_dict[techs_ss] = eval(redis_hook_2.get("kpi_provis_prev_state_%s"%(techs_ss)))


#############################################SS UL ISSUE###############################################################################################
def calculate_wimax_ss_provision(wimax_ul_rssi,wimax_dl_rssi,wimax_dl_cinr,wimax_ss_ptx_invent):
	ss_state = "normal"
	try:
		if (wimax_ul_rssi !=None and wimax_dl_rssi !=None and wimax_ss_ptx_invent != None and (int(wimax_ul_rssi) < -83 or \
			int(wimax_dl_rssi) < -83 and int(wimax_ss_ptx_invent) > 20)):
			ss_state = "los"
			state = 0
			state_string= "ok"
		elif (wimax_ul_rssi !=None and wimax_dl_rssi != None and wimax_ss_ptx_invent != None and (int(wimax_ul_rssi) < -83 or \
			int(wimax_dl_rssi) < -83 and int (wimax_ss_ptx_invent) <= 20)):
			ss_state = "need_alignment"
			state = 0
			state_string= "ok"
		elif wimax_dl_cinr != None and int(wimax_dl_cinr) <= 15:
			ss_state = "rogue_ss"
			state = 0
			state_string = "ok"
		else:
			ss_state = "normal" 
			state_string= "ok"
			state = 0
	   	return ss_state
	except Exception:
		logging.error("Error in Calculating Wimax SS provisioning KPI")	   
		return ss_state





def calculate_cambium_ss_provision(cambium_ul_rssi,cambium_dl_rssi,cambium_dl_jitter,cambium_ul_jitter,cambium_rereg_count):
	ss_state = "normal"
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

def calculate_radwin5k_ss_provision(radwin_rssi,radwin_uas):
	ss_state = "normal" 
	try:
		if radwin_rssi != None and int(radwin_rssi) < -80:
			ss_state = "los"
			state = 0
			state_string= "ok"
		elif radwin_uas != None and int(radwin_uas) != 900 and int(radwin_uas) > 20:
			ss_state = "uas"
			state = 0
			state_string= "ok"
		else:
			ss_state = "normal" 
			state_string= "ok"
			state = 0

		return ss_state
	except Exception ,e:
		logging.error("Error in Calculating Radwin5K SS provisioning KPI")
		return ss_state


# age_of_state = age_since_last_state(host_name, args['service'], state_string)
# service_dict = service_dict_for_kpi_services(
# 	perf,state_string,host_name,
# 	site,ip_address,age_of_state,**args)
# service_dict_list.append(service_dict)










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

def calculate_age(hostname,current_severity,device_type,current_time):

	try:

		previous_device_state = all_device_type_age_dict.get(device_type)
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
				logging.error("Got the devices %s but unable to fetch the since key ,its either None or Empty"%(hostname))
				return 0

		elif device_state.get("state") != current_severity:
			return current_time
			

	except Exception:
		logging.error("Unable to get state for device %s will be created when updating refer"%(hostname))
		traceback.print_exc()
		return 0
		#traceback.print_exc()


def backtrack_x_min(x,seconds):
	value= int(math.floor(x / seconds)) * seconds
	return value
