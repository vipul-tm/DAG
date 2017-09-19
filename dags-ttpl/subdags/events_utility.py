###############Utility functions for event################
import re
from airflow.models import Variable
from os import listdir
from os.path import isfile, join
import time
import logging
import traceback
from airflow.hooks import  MemcacheHook
from airflow.hooks import RedisHook
from subdags.format_utility import get_device_type_from_name
from subdags.format_utility import get_previous_device_states


redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")

redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
OKGREEN = '\033[92m'
NC='\033[0m'
events_pl_rta_list = []
def get_device_alarm_tuple(network_data,event_rules):
	start_time = time.time()
	all_devices_states_pl = get_previous_device_states(redis_hook_5)
	all_devices_states_rta = get_previous_device_states(redis_hook_5,"rta")

	
	for device_data in network_data:
		device_data = eval(device_data)
		hostname = device_data.get('host')
		severity = device_data.get('severity')
		ip_address = device_data.get("ip_address")
		datasource = device_data.get('ds')	
		if datasource == "pl":
			all_devices_states = all_devices_states_pl
		elif datasource == "rta":
			all_devices_states = all_devices_states_rta
		else:
			logging.info("DATASOURCE is neither pl nor rta")

		local_timestamp = int(device_data.get("local_timestamp")/300)*300
		try:
			prev_state = all_devices_states.get(hostname).get('state')
		except Exception:
			logging.info("No previous state found for HOST %s"%hostname)
			
			#traceback.print_exc()
			
			
		if prev_state.lower() != severity.lower() and prev_state != None:
			 #TODO:Send clear trap here of the previous severity
			device_type = get_device_type_from_name(hostname)
			datasource = device_data.get('ds')				   
			event_rules_key = device_type+"_"+datasource
			try:
				device_events = event_rules.get(event_rules_key)
				previous_event = device_events.get(prev_state).get('event-name')
				current_event = device_events.get(severity).get('event-name')
				mat_severity = device_events.get(severity).get('mat_severity')
			except Exception:
				logging.error(OKGREEN+"problem in fetching the data for the device"+ event_rules_key+NC)
				traceback.print_exc()
			if severity.lower() == 'ok' or severity.lower() == 'up':
				#send only trap of clear for prev sev
				trap =('',previous_event , '',ip_address,
							'',
							device_type,
							mat_severity,
							'',
							 time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(local_timestamp)), #check_time,
							''
						)
				events_pl_rta_list.append(trap)

			elif severity.lower() != 'ok' and severity.lower() != 'up':
				#send 2 traps 1 of clear of prev_severity adn one of new severity
				
				trap_clear = ('',previous_event , '',ip_address,
							'',
							device_type,
							'clear',
							'',
							 time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(local_timestamp)), #check_time,
							''
						)
				trap_current = ('', current_event, '',ip_address,
					 				'',
					 				device_type,
					 				mat_severity,
					 				'',
									time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(float(local_timestamp))),
					 				''
					 				)
				events_pl_rta_list.append(trap_clear)
				events_pl_rta_list.append(trap_current)
			else:
				logging.error("The severities are diffrent but not ok")
				

		else:
			logging.info("No Change in severity")
			continue
	logging.info("TIME EVENT: %s"%(time.time() - start_time) )
	logging.info("Returning %s alarm traps for "%(len(events_pl_rta_list)))
	return events_pl_rta_list

def update_last_device_down(redis_hook=redis_hook_4):
	all_devices_down_states = get_previous_device_states(redis_hook_5,"down")
	aggregated_data_vals = list(redis_hook.get_keys("nw_agg_nocout_*"))

	
	for key in aggregated_data_vals:
		logging.info("Gettting For %s"%(key))
		data = redis_hook.rget(key)
		for slot in data:
			slot=eval(slot)
			for device in slot:
				device = eval(device)
				new_host = str(device.get("host"))
				new_state = str(device.get("severity"))
				new_refer = str(device.get("refer"))
				datasource = str(device.get("ds"))

				if datasource == "pl":
					try:
						device_old_state = all_devices_down_states.get(new_host)	
						old_state = device_old_state.get('state')
						old_refer = device_old_state.get('since')

						if new_state != old_state:
							if new_state == "up" and old_state == "down":
								all_devices_down_states[new_host] = {'state':new_state,'since':old_refer}
							if new_state == "down" and old_state == "up":
								all_devices_down_states[new_host] = {'state':new_state,'since':new_refer}
							
						else:

							all_devices_down_states[new_host] = {'state':old_state,'since':old_refer}
					except Exception:
						logging.warning("Unable to find host %s in old state for host " %new_host)
						if new_state == "up":
							all_devices_down_states[new_host] = {'state':new_state,'since':old_refer}
							logging.info("Created new up state dict for %s as severity %s and since %s"%(new_host,new_state,old_refer))
						elif new_state == "down":
							all_devices_down_states[new_host] = {'state':new_state,'since':new_refer}
							logging.info("Created new down state dict for %s as severity %s and since %s"%(new_host,new_state,new_refer))
						else:
							logging.info("Other State than up or down")
	try:

		redis_hook_5.set("all_devices_down_state",str(all_devices_down_states))

		logging.info("Successfully inserted All(%s) the values in redis"%(len(all_devices_down_states)))
	except Exception:
		logging.error("Unable to update last device down ")
		traceback.print_exc()




def update_device_state_values(redis_hook=redis_hook_4):
	try:
		logging.info("We get all the devices and compare what all states have changed")
		old_states_pl = get_previous_device_states(redis_hook_5)
		old_states_rta = get_previous_device_states(redis_hook_5,"rta")
	

		aggregated_data_vals = list(redis_hook.get_keys("nw_agg_nocout_*"))

		for key in aggregated_data_vals:
			data = redis_hook.rget(key)
			for slot in data:
				slot=eval(slot)
				for device in slot:
					device = eval(device)
					new_host = str(device.get("host"))
					new_sev = str(device.get("severity"))
					new_refer = str(device.get("refer"))
					datasource = str(device.get("ds"))
					if datasource == "pl":
						old_states = old_states_pl
						try:						
							device_old_state = old_states.get(new_host)						
							old_state = device_old_state.get('state')
							old_refer = device_old_state.get('since')
							if old_state!= new_sev:
								#logging.info("State has changed for %s from %s to %s"%(new_host,old_state,new_sev))
								old_states[new_host] = {'state':new_sev,'since':new_refer}
							else:
								old_states[new_host] = {'state':old_state,'since':old_refer}
							#logging.info("State is same for %s as %s"%(new_host,old_state))
						except Exception:
							logging.info("Unable to find host in old state for host %s " %new_host)
							old_states[new_host] = {'state':new_sev,'since':new_refer}
							logging.info("Created new sate dict for %s as severity %s and since %s"%(new_host,new_sev,new_refer))
							continue



					elif datasource == "rta":
						old_states = old_states_rta

						try:						
							device_old_state = old_states.get(new_host)						
							old_state = device_old_state.get('state')
							old_refer = device_old_state.get('since')
							if old_state!= new_sev and old_state != None:
								#logging.info("State has changed for %s from %s to %s"%(new_host,old_state,new_sev))
								old_states[new_host] = {'state':new_sev,'since':new_refer}
							else:
								old_states[new_host] = {'state':old_state,'since':old_refer}
							#logging.info("State is same for %s as %s"%(new_host,old_state))
						except Exception:
							logging.info("Unable to find host in old state for host %s"%new_host)
							logging.info(new_refer)
							logging.info(new_sev)
							old_states[new_host] = {'state':new_sev,'since':new_refer}
							logging.info("Created new sate dict for %s as severity %s and since %s"%(new_host,new_sev,new_refer))
							continue


					else:
						logging("The Datasource extracted from device from device slot in redis is neither PL nor RTA")

					
		try:				
			logging.info("PL State: %s RTA states : %s"%(len(old_states_pl),len(old_states_rta)))
			if old_states_pl != None and len(old_states_pl) > 0:
				redis_hook_5.set("all_devices_state",str(old_states_pl))
				logging.info("Succeessfully Updated PL DS last state")
			if old_states_rta != None and len(old_states_rta) > 0:
				redis_hook_5.set("all_devices_state_rta",str(old_states_rta))
				logging.info("Succeessfully Updated RTA DS last state")
			else:
				logging.info("recieved fucking None")	
		except Exception:
			logging.error("Unable to actually insert te updated data into redis")

	except Exception:
		logging.info("Unable to get latest refer")
		traceback.print_exc()
	#if len(latest_refer) > 0:
	#	redis_hook.set("all_devices_state","")
	#	logging.info("Initialized old refer to empty")	
	#	redis_hook.set("all_devices_state",str(latest_refer))
	#	logging.info("Succsexxfully Updated old refer with latest refer.")

	#else:
	#	logging.info("Data not present in latest refer quitting.")