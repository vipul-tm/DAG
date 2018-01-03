###############Utility functions for INterference################
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
redis_hook_6 = RedisHook(redis_conn_id="redis_hook_10")

rules = eval(Variable.get('rules'))
ul_tech = eval(Variable.get('interference_kpi_technologies'))
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
operators = eval(Variable.get('operators'))
service_state_type = ['warning','critical']
all_device_type_age_dict ={}

try:
	for techs_bs in ul_tech:
		techs_ss = ul_tech.get(techs_bs)
		all_device_type_age_dict[techs_ss] = eval(redis_hook_2.get("kpi_interference_prev_state_%s"%(techs_ss)))
		all_device_type_age_dict[techs_bs] = eval(redis_hook_2.get("kpi_interference_prev_state_%s"%(techs_bs)))
except Exception:
	pass
#############################################SS INtERFERENCE###############################################################################################
def calculate_cambium_all_ss_dl_interference(cambium_beacon):
	dl_interference = 0
	if cambium_beacon and cambium_beacon != None:
		
		if 0 in cambium_beacon:
			dl_interference = 1

		return dl_interference
	else:
		return 404
	


def calculate_cambium_ss_ul_interference(cambium_ul_rssi,cambium_reg,cambium_session_uptime,cambium_device_uptime):
	try:
		interference=0
		rssi_check = False
		reg_check = False
		session_uptime_check = False
		device_uptime_check = False
		if cambium_ul_rssi and cambium_ul_rssi != None:
			for ul_rssi in cambium_ul_rssi:
				if ul_rssi < -75:
					rssi_check = True
		if cambium_reg and len(cambium_reg) == 3:
			if int(cambium_reg[1]) - int(cambium_reg[0]) > 0:
				reg_check = True
		if cambium_session_uptime and len(cambium_session_uptime) == 3:
			if check_session_uptime_difference(cambium_session_uptime):
				session_uptime_check = True
		if cambium_device_uptime and cambium_session_uptime and len(cambium_device_uptime) == 3 and len(cambium_session_uptime) == 3:
			if check_device_uptime_difference(cambium_session_uptime,cambium_device_uptime):
				device_uptime_check = True


		if rssi_check and (reg_check or session_uptime_check) and device_uptime_check:
			interference = 1
		

		return interference


	except Exception as e :
		logging.error("Error in Cambium100 SS UL interference")
		traceback.print_exc()
		return 404


def calculate_cambium_i_and_m_ss_ul_interference(cambium_ul_snr,cambium_reg,cambium_session_uptime,cambium_device_uptime):
	if cambium_ul_snr and cambium_reg and cambium_session_uptime and  cambium_device_uptime:
		try:
			

			interference=0
			snr_check = False
			reg_check = False
			session_uptime_check = False
			device_uptime_check = False
			
			if cambium_ul_snr and cambium_ul_snr != None:
				for ul_rssi in cambium_ul_snr:
					if cambium_ul_snr < 20:
						snr_check = True

			if cambium_reg and len(cambium_reg) == 3:
				
				if int(cambium_reg[1]) - int(cambium_reg[0]) > 0 and int(cambium_reg[2]) - int(cambium_reg[1]) > 0:
					reg_check = True
			else:
				return 404
			
			if cambium_session_uptime and len(cambium_session_uptime) > 3:
				if check_session_uptime_difference(cambium_session_uptime):
					session_uptime_check = True
			else:
				return 404
			
			if cambium_device_uptime and cambium_session_uptime and len(cambium_device_uptime) > 3 and len(cambium_session_uptime) > 3:
				if check_device_uptime_difference(cambium_session_uptime,cambium_device_uptime):
					device_uptime_check = True
			else:
				return 404

			
			if (snr_check or reg_check or session_uptime_check) and device_uptime_check:
				interference = 1
			

			return interference


		except Exception as e :
			logging.error("Error in Cambium100 SS UL interference")
			traceback.print_exc()
			
			return 404
	else:
		
		return 404



#############################################BS INTERFERENCE###############################################################################################
def calculate_cambium_bs_interference(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable interference_services_mapping
	bs_interference_dict = {'pmp1':0}
	bs_interference = 0
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
									bs_interference = bs_interference+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_interference = bs_interference + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					#print "EXCEPTI0N"
					bs_interference = bs_interference + 0
				
			bs_interference_percent = float((bs_interference/float(total_ss_len))*100)

	

			bs_interference_dict['pmp1'] = round(bs_interference_percent,2)
			
		else:
			#print "No SS Connected "
			bs_interference_dict['pmp1'] = 404



		return bs_interference_dict

	else:
		bs_interference_dict['pmp1'] = 404
		return bs_interference_dict




#Its exactly same as cambium will change it soon to collabrate in one
def calculate_cambiumi_bs_interference(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable interference_services_mapping
	bs_interference_dict = {'pmp1':0}
	bs_interference = 0
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
									bs_interference = bs_interference+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_interference = bs_interference + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					print "EXCEPTI0N"
					bs_interference = bs_interference + 0
					
			bs_interference_percent = float((bs_interference/float(total_ss_len))*100)
			if bs_interference_percent > 100:
				bs_interference_percent = 100

			bs_interference_dict['pmp1'] = round(bs_interference_percent,2)
			
		else:
			#print "No SS Connected "
			bs_interference_dict['pmp1'] = 405



		return bs_interference_dict

	else:
		return bs_interference_dict
#this is exactly same as Rad5k and cambium but only diffrence inn function name :P 
def calculate_cambiumm_bs_interference(ss_data,bs,bs_services,ss_services):
	#serv_name = bs.get(service) #this is sec_id as written in variable interference_services_mapping
	bs_interference_dict = {'pmp1':0}
	bs_interference = 0
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
									bs_interference = bs_interference+1
									

								
					else:
						#print "Didnt find the connected SS in the bucket yet"
						bs_interference = bs_interference + 0
						redis_hook_6.rpush('lost_ss_queue',bs)
						
						#here SS is not found possible it's onto some other site due to asshole support people lets create a bucket for them

				except Exception:
					# SAME SHIT HERE too
					print "EXCEPTI0N"
					bs_interference = bs_interference + 0
					
			bs_interference_percent = float((bs_interference/float(total_ss_len))*100)
			if bs_interference_percent > 100:
				bs_interference_percent = 100

			bs_interference_dict['pmp1'] = round(bs_interference_percent,2)
			
		else:
			#print "No SS Connected "
			bs_interference_dict['pmp1'] = 405



		return bs_interference_dict

	else:
		return bs_interference_dict





















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

def convert_uptime(uptime):
	uptime = int(uptime)
   	uptime = uptime / 100
	seconds = uptime % 60
	rem = uptime / 60
	minutes = rem % 60
	hours = (rem % 1440) / 60
	days = rem / 1440
	now = int(time.time())
	#since = time.strftime("%c", time.localtime(now - uptime))
	since = time.localtime(now - uptime)
	return since
   	#state = 0
	#infotext = "up since %s (%dd %02d:%02d:%02d)" % (since, days, hours, minutes, seconds)
	#return infotext

def check_session_uptime_difference(session_uptime):
	if int(session_uptime[3]) - int(session_uptime[0]) < 900:
		return True
	else:
		return False

def check_device_uptime_difference(session_uptime,device_uptime):
	if int(session_uptime[3]) - int(device_uptime[3]) > 40:
		return True
	else:
		return False

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
