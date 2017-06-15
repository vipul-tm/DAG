import traceback
import logging
import time


def process_topology_data(data,device_down_list,tech,site):
	invent_check_list = []
	ss_sec_id = []
	invent_service_dict = {}
	matching_criteria = {}
	topology_list=[]
	topology_dict ={}
	topology_dict['refer'] = 'unknown'
	for entry in data:
		entry = eval(entry)
		
		try:
			if str(entry[0]) in device_down_list:
				logging.info("Device %s is down"%entry[0])
				continue

			service_state = entry[4]
			host = entry[0]
			
			if service_state == 0:
				service_state = "OK"
			elif service_state == 1:
				service_state = "WARNING"
			elif service_state == 2:
				service_state = "CRITICAL"
			elif service_state == 3:
				service_state = "UNKNOWN"
			host_ip = entry[1]
			service = entry[3]
			perf_data_output = entry[5]
			machine_name = site.split("_")[0]
			if 'wimax' in tech.lower() and perf_data_output:
				#logging.info("Formatting Wimax")
				
				plugin_output = (perf_data_output.split('- ')[1])
				plugin_output = [mac for mac in plugin_output.split(' ')]
				ss_mac  = map(lambda x: x.split('=')[0],plugin_output)
				ss_ip  = map(lambda x: x.split('=')[1].split(',')[9],plugin_output)
				ss_sec_id  = map(lambda x: x.split('=')[1].split(',')[8],plugin_output)
				port_number =  map(lambda x: x.split('=')[1].split(',')[10],plugin_output)
				port_number = ['odu'+str(each)  for each in port_number]
				ds="topology"
				current_time = int(time.time())
				topology_dict = dict (sys_timestamp=current_time,check_timestamp=current_time,device_name=str(host),
								service_name=service,data_source=ds,site_name=site,ip_address=host_ip,machine_name=machine_name,mac_address='')
				
				for i,ip in enumerate(ss_ip):
 					topology_dict['sector_id'] = ss_sec_id[i]
 					topology_dict['connected_device_ip'] = ss_ip[i]
 					topology_dict['connected_device_mac'] = ss_mac[i]
 					topology_dict['refer'] = port_number[i]
					topology_list.append(topology_dict.copy())


			elif 'rad5k' in tech.lower() and perf_data_output:
				logging.info("Formatting rad5k")
				plugin_output = str(perf_data_output.split('- ')[1])
				plugin_output = [mac for mac in plugin_output.split(' ')]
				ap_sector_id = plugin_output[2]
				ap_mac= plugin_output[0]
				ss_ip_mac = filter(lambda x: '/' in x,plugin_output)
				ss_ip= map(lambda x: x.split('/')[-1],ss_ip_mac)
				ss_mac = map(lambda x: x.split('/')[0],ss_ip_mac)
				ss_mac = map(lambda x: x.split('=')[0],ss_mac)
				ds="topology"
				current_time = int(time.time())
 				topology_dict = dict (sys_timestamp=current_time,check_timestamp=current_time,device_name=str(host),
								service_name=service,data_source=ds,site_name=site,ip_address=host_ip,machine_name=machine_name)
 				for i,ip in enumerate(ss_ip):
 					topology_dict['sector_id'] = ss_sec_id[i]
 					topology_dict['mac_address'] = ap_mac[i]
 					topology_dict['connected_device_ip'] = ss_ip[i]
 					topology_dict['connected_device_mac'] = ss_mac[i]
 					topology_dict['refer'] = port_number[i]
					topology_list.append(topology_dict.copy())


			elif 'cambium' in tech.lower() and perf_data_output:
				plugin_output = str(perf_data_output.split('- ')[1])
				plugin_output = [mac for mac in plugin_output.split(' ')]
				ap_sector_id = plugin_output[1]
				ap_mac= plugin_output[0]
				ss_ip_mac = filter(lambda x: '/' in x,plugin_output)
				ss_ip= map(lambda x: x.split('/')[0],ss_ip_mac)
				ss_mac = map(lambda x: x.split('/')[1],ss_ip_mac)
				ds="topology"
				current_time = int(time.time())
				topology_dict = dict (sys_timestamp=current_time,check_timestamp=current_time,device_name=str(host),
								service_name=service,data_source=ds,site_name=site,ip_address=host_ip,machine_name=machine_name)

				for i,ip in enumerate(ss_ip):
 					topology_dict['sector_id'] = ap_sector_id[i]
 					topology_dict['mac_address'] = ap_mac[i]
 					topology_dict['connected_device_ip'] = ss_ip[i]
 					topology_dict['connected_device_mac'] = ss_mac[i]
 					topology_dict['refer'] = ''
					topology_list.append(topology_dict.copy())
				


			elif 'cam450i' in tech.lower() and perf_data_output:
				plugin_output = str(perf_data_output.split('- ')[1])
				plugin_output =  plugin_output.split(' ')
				ap_sector_id = plugin_output[1]
				ap_mac= plugin_output[0]
				ss_ip_mac = filter(lambda x: '/' in x,plugin_output)
				ss_ip= map(lambda x: x.split('/')[-1],ss_ip_mac)
				ss_mac = map(lambda x: x.split('/')[0],ss_ip_mac)
				ds="topology"
				current_time = int(time.time())
				topology_dict = dict (sys_timestamp=current_time,check_timestamp=current_time,device_name=str(host),
								service_name=service,data_source=ds,site_name=site,ip_address=host_ip,machine_name=machine_name)
				for i,ip in enumerate(ss_ip):
 					topology_dict['sector_id'] = ap_sector_id[i]
 					topology_dict['mac_address'] = ap_mac[i]
 					topology_dict['connected_device_ip'] = ss_ip[i]
 					topology_dict['connected_device_mac'] = ss_mac[i]
 					topology_dict['refer'] = ''
					topology_list.append(topology_dict.copy())
			else:
				logging.info("Technology not found for UL ISSUE or no perf data")
				continue
				
		except Exception:
			logging.info("Ommiting device host :  %s"%host)
			#traceback.print_exc()
			continue
	logging.info("Returning topo of length %s for tech %s" %(len(topology_list),tech))
	return topology_list