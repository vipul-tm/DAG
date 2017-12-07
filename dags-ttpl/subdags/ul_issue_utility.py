###############Utility functions for UL_issue################
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
redis_hook_6 = RedisHook(redis_conn_id="redis_hook_6")
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
rules = eval(Variable.get('rules'))
ul_tech = eval(Variable.get('ul_issue_kpi_technologies'))
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
operators = eval(Variable.get('operators'))
service_state_type = ['warning','critical']
all_device_type_age_dict ={}

for techs_bs in ul_tech:
	techs_ss = ul_tech.get(techs_bs)
	all_device_type_age_dict[techs_ss] = eval(redis_hook_2.get("kpi_ul_prev_state_%s"%(techs_ss)))
	all_device_type_age_dict[techs_bs] = eval(redis_hook_2.get("kpi_ul_prev_state_%s"%(techs_bs)))

#############################################SS UL ISSUE###############################################################################################
def calculate_wimax_ss_ul_issue(wimax_dl_intrf,wimax_ul_intrf):
	
	if wimax_ul_intrf and wimax_dl_intrf:
		
		if len(wimax_dl_intrf) == 2 and wimax_dl_intrf[0].lower() == 'critical' and wimax_dl_intrf[1].lower() == 'critical':
			ul_issue = 0 
			state_string = "ok"
		elif len(wimax_ul_intrf) == 2 and wimax_ul_intrf[0].lower() in service_state_type and wimax_ul_intrf[1].lower() in service_state_type:
			ul_issue = 1
			state_string = "ok"
		elif len(wimax_ul_intrf) == 2  and len(wimax_dl_intrf) == 2:
			ul_issue = 0
			state_string = "ok"
		else:
			ul_issue = 0
			state_string = "unknown"
		return ul_issue
	else:
		logging.info("State None Found %s and %s"%(wimax_ul_intrf,wimax_dl_intrf))
		return 0

def calculate_cambium_ss_ul_issue(cambium_ul_jitter,cambium_rereg_count):
	ul_jitter_count = 0
	rereg_count = 0

	if cambium_rereg_count and cambium_ul_jitter:
		rereg_values= cambium_rereg_count
		ul_jitter_values= cambium_ul_jitter

		try:
			for entry in ul_jitter_values:
				if entry in service_state_type:
					ul_jitter_count = ul_jitter_count +1
		except:
			ul_jitter_count = 0
		for entry in rereg_values:
			if entry in service_state_type:
				rereg_count = rereg_count + 1

		
		if ul_jitter_count == 2 or rereg_count == 2 :
			state_string = "ok"
			state = 0
			ul_issue = 1
		else:
			state_string = "ok"

			ul_issue = 0	

		return ul_issue
	else:
		return 0

def calculate_radwin5k_ss_ul_issue(rad5k_ss_dl_uas,rad5k_ss_ul_modulation):
	try:
		ul_issue=0
		if rad5k_ss_dl_uas and rad5k_ss_ul_modulation:
			if len(rad5k_ss_dl_uas) == 2:
				if len([i for i in rad5k_ss_dl_uas if int(i) >0])==2 :
					ul_issue = 1
				if len(rad5k_ss_ul_modulation) == 2:
					if len([i for i in rad5k_ss_ul_modulation if i == 'BPSK-FEC-1/2'])==2 :
						ul_issue = 1
		
			return ul_issue
		else:
			print("No Services Found")
			return 0
	except Exception as e :
		logging.error("Error in rad5k ul issue")
		traceback.print_exc()
		return '405'

def calculate_radwin5kjet_ss_ul_issue(rad5kjet_ss_dl_uas,rad5kjet_ss_ul_modulation):
	try:
		ul_issue=0
		if rad5kjet_ss_dl_uas and rad5kjet_ss_ul_modulation:
			if len(rad5kjet_ss_dl_uas) == 2:
				if len([i for i in rad5kjet_ss_dl_uas if int(i) >0])==2 :
					ul_issue = 1
				if len(rad5kjet_ss_ul_modulation) == 2:
					if len([i for i in rad5kjet_ss_ul_modulation if i == 'BPSK-FEC-1/2'])==2 :
						ul_issue = 1
		
			return ul_issue
		else:
			print("No Services Found")
			return 0
	except Exception as e :
		logging.error("Error in rad5k ul issue")
		traceback.print_exc()
		return '405'


# age_of_state = age_since_last_state(host_name, args['service'], state_string)
# service_dict = service_dict_for_kpi_services(
# 	perf,state_string,host_name,
# 	site,ip_address,age_of_state,**args)
# service_dict_list.append(service_dict)



#############################################BS UL ISSUE###############################################################################################


def calculate_wimax_bs_ul_issue(ss_data,bs,bs_services,ss_services):

	#serv_name = bs.get(service) #this is sec_id as written in variable ul_issue_services_mapping
	bs_ul_issue_dict = {'pmp1':0,'pmp2':0}
	
	if bs.get("connectedss") and bs.get("connectedss") != None:
		for pmp_port in range(1,len(bs_services)+1):
			bs_ul_issue = 0 #this is to reset the ul issue for the next port
			if len(bs.get("connectedss").get(pmp_port)) > 0:
				try:
					
					connected_ss = bs.get("connectedss").get(pmp_port)
					total_ss_len = len(connected_ss)
					
				except AttributeError:
					print "EXCEPTION %s"%(bs)
					
				for ss in connected_ss:
					try:
						
						if ss_data.get(ss):
							
							ss_processed_data = ss_data.get(ss)
							for ss_service in ss_services:
								if ss_processed_data.get(ss_service):
									if ss_processed_data.get(ss_service) == 1:
										bs_ul_issue = bs_ul_issue+1
										

									
						else:
							#print "Didnt find the connected SS in the bucket yet"
							bs_ul_issue = bs_ul_issue + 0
							redis_hook_6.rpush('lost_ss_queue',bs)
							
							#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

					except Exception:
						# SAME SHIT HERE too
						print "EXCEPTI0N"
						bs_ul_issue = bs_ul_issue + 0
				
				bs_ul_issue_percent = float((bs_ul_issue/float(total_ss_len))*100)
				if bs_ul_issue_percent > 100:
					bs_ul_issue_percent = 100
					
				bs_ul_issue_dict['pmp%s'%(pmp_port)] = round(bs_ul_issue_percent,2)
				
			else:
				#print "No SS Connected "
				bs_ul_issue_dict['pmp%s'%(pmp_port)] = 405

		

		return bs_ul_issue_dict

	else:
		return bs_ul_issue_dict


def calculate_cambium_bs_ul_issue(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable ul_issue_services_mapping
	bs_ul_issue_dict = {'pmp1':0}
	bs_ul_issue = 0
	if bs.get("connectedss") and bs.get("connectedss") != None:
	
		if len(bs.get("connectedss") )> 0:
			try:
				
				connected_ss = bs.get("connectedss")
				total_ss_len = len(connected_ss)
				
			except AttributeError:
				print "EXCEPTION %s"%(bs)
				
			for ss in connected_ss:
				try:
					
					if ss_data.get(ss):
						ss_processed_data = ss_data.get(ss)
						for ss_service in ss_services:
							if ss_processed_data.get(ss_service):
								if ss_processed_data.get(ss_service) == 1:
									bs_ul_issue = bs_ul_issue+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_ul_issue = bs_ul_issue + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					#print "EXCEPTI0N"
					bs_ul_issue = bs_ul_issue + 0
				
			bs_ul_issue_percent = float((bs_ul_issue/float(total_ss_len))*100)

			if bs_ul_issue_percent > 100:
				bs_ul_issue_percent = 100

			bs_ul_issue_dict['pmp1'] = round(bs_ul_issue_percent,2)
			
		else:
			#print "No SS Connected "
			bs_ul_issue_dict['pmp1'] = 405



		return bs_ul_issue_dict

	else:
		return bs_ul_issue_dict




#Its exactly same as cambium will change it soon to collabrate in one
def calculate_radwin5k_bs_ul_issue(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable ul_issue_services_mapping
	bs_ul_issue_dict = {'pmp1':0}
	bs_ul_issue = 0
	if bs.get("connectedss") and bs.get("connectedss") != None:
	
		if len(bs.get("connectedss") )> 0:
			try:
				
				connected_ss = bs.get("connectedss")
				total_ss_len = len(connected_ss)
				
			except AttributeError:
				print "EXCEPTION %s"%(bs)
				
			for ss in connected_ss:
				try:
					
					if ss_data.get(ss):
						ss_processed_data = ss_data.get(ss)
						for ss_service in ss_services:
							if ss_processed_data.get(ss_service):
								if ss_processed_data.get(ss_service) == 1:
									bs_ul_issue = bs_ul_issue+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_ul_issue = bs_ul_issue + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					print "EXCEPTI0N"
					bs_ul_issue = bs_ul_issue + 0
					
			bs_ul_issue_percent = float((bs_ul_issue/float(total_ss_len))*100)
			if bs_ul_issue_percent > 100:
				bs_ul_issue_percent = 100

			bs_ul_issue_dict['pmp1'] = round(bs_ul_issue_percent,2)
			
		else:
			#print "No SS Connected "
			bs_ul_issue_dict['pmp1'] = 405



		return bs_ul_issue_dict

	else:
		return bs_ul_issue_dict
#this is exactly same as Rad5k and cambium but only diffrence inn function name :P 
def calculate_radwin5kjet_bs_ul_issue(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable ul_issue_services_mapping
	bs_ul_issue_dict = {'pmp1':0}
	bs_ul_issue = 0
	if bs.get("connectedss") and bs.get("connectedss") != None:
	
		if len(bs.get("connectedss") )> 0:
			try:
				
				connected_ss = bs.get("connectedss")
				total_ss_len = len(connected_ss)
				
			except AttributeError:
				print "EXCEPTION %s"%(bs)
				
			for ss in connected_ss:
				try:
					
					if ss_data.get(ss):
						ss_processed_data = ss_data.get(ss)
						for ss_service in ss_services:
							if ss_processed_data.get(ss_service):
								if ss_processed_data.get(ss_service) == 1:
									bs_ul_issue = bs_ul_issue+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_ul_issue = bs_ul_issue + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					print "EXCEPTI0N"
					bs_ul_issue = bs_ul_issue + 0
					
			bs_ul_issue_percent = float((bs_ul_issue/float(total_ss_len))*100)
			if bs_ul_issue_percent > 100:
				bs_ul_issue_percent = 100

			bs_ul_issue_dict['pmp1'] = round(bs_ul_issue_percent,2)
			
		else:
			#print "No SS Connected "
			bs_ul_issue_dict['pmp1'] = 405



		return bs_ul_issue_dict

	else:
		return bs_ul_issue_dict





















		##########################################################################HELPERS##########################################



		# 								  _______  __	   _______  _______  _______  _______
		#						|\	 /| (  ____ \( \	  (  ____ )(  ____ \(  ____ )(  ____ \
		#						| )   ( || (	\/| (	  | (	)|| (	\/| (	)|| (	\/
		#						| (___) || (__	| |	  | (____)|| (__	| (____)|| (_____ 
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

def forward_x_min(x,seconds):
	value= int(math.ceil(x / seconds)) * seconds
	return value
