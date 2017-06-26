from airflow.models import Variable
from airflow.operators import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks import RedisHook
from airflow.hooks import  MemcacheHook
import socket
import logging
import csv #for debugging task
import time
SQLhook=MySqlHook(mysql_conn_id='application_db')
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
#redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
databases = eval(Variable.get('databases'))
#############################################HELPERS################################################
def dict_rows(cur):
    desc = cur.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cur.fetchall()
    ]

def execute_query(query):
	conn = SQLhook.get_conn()
	cursor = conn.cursor()
	cursor.execute(query)
	data =  dict_rows(cursor)
	cursor.close()
	return data

def createDict(data):
	#TODOL There are 3 levels of critality handle all those(service_critical,critical,dtype_critical)
	rules = {}
	ping_rule_dict = {}
	operator_name_with_operator_in = eval(Variable.get("special_operator_services")) #here we input the operator name in whcih we wish to apply IN operator
	service_name_with_operator_in = []
	for operator_name in operator_name_with_operator_in:
		service_name = "_".join(operator_name.split("_")[:-1])
		service_name_with_operator_in.append(service_name)


	for device in data:
		service_name = device.get('service')
		device_type = device.get('devicetype')
		if device.get('dtype_ds_warning') and device.get('dtype_ds_critical'):
			device['critical'] = device.get('dtype_ds_critical')
			device['warning'] = device.get('dtype_ds_warning')
		elif device.get('service_warning') and device.get('service_critical'):
			device['critical'] = device.get('service_critical')
			device['warning'] = device.get('service_warning')
		
		if service_name:
			name =  str(service_name)
			rules[name] = {}
		if device.get('critical'):
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than', 'value': device.get('critical') or device.get('dtype_ds_critical')}]}
		else:
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than', 'value': ''}]}
		if device.get('warning'):
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than', 'value': device.get('warning') or device.get('dtype_ds_warning')}]})
		else:
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than', 'value': ''}]})
		if device_type not in ping_rule_dict:
			if device.get('ping_pl_critical') and device.get('ping_pl_warning') and device.get('ping_rta_critical') and device.get('ping_rta_warning'):
				 ping_rule_dict[device_type] = {
				'ping_pl_critical' : device.get('ping_pl_critical'),
				'ping_pl_warning': 	 device.get('ping_pl_warning') ,
				'ping_rta_critical': device.get('ping_rta_critical'),
				'ping_rta_warning':  device.get('ping_rta_warning')
				}
	for device_type in  ping_rule_dict:
		if ping_rule_dict.get(device_type).get('ping_pl_critical'):
			rules[device_type+"_pl"]={}
			rules[device_type+"_pl"].update({"Severity1":["critical",{'name': device_type+"_pl_critical", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_pl_critical')) or ''}]})
			
		if ping_rule_dict.get(device_type).get('ping_pl_warning'):
			rules[device_type+"_pl"].update({"Severity2":["warning",{'name': device_type+"_pl_warning", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_pl_warning')) or ''}]})
			rules[device_type+"_pl"].update({"Severity3":["up",{'name': device_type+"_pl_up", 'operator': 'less_than', 'value': int(ping_rule_dict.get(device_type).get('ping_pl_warning')) or ''},'AND',{'name': device_type+"_pl_up", 'operator': 'greater_than_equal_to', 'value': 0}]})
			rules[device_type+"_pl"].update({"Severity4":["down",{'name': device_type+"_pl_down", 'operator': 'equal_to', 'value': 100}]})
			
		if ping_rule_dict.get(device_type).get('ping_rta_critical'):
			rules[device_type+"_rta"] = {}
			rules[device_type+"_rta"].update({"Severity1":["critical",{'name': device_type+"_rta_critical", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_rta_critical')) or ''}]})
		if ping_rule_dict.get(device_type).get('ping_rta_warning'):
			rules[device_type+"_rta"].update({"Severity2":["warning",{'name': device_type+"_rta_warning", 'operator': 'greater_than', 'value': int(ping_rule_dict.get(device_type).get('ping_rta_warning')) or ''}]})
			rules[device_type+"_rta"].update({"Severity3":["ok",{'name': device_type+"_rta_up", 'operator': 'less_than', 'value': int(ping_rule_dict.get(device_type).get('ping_rta_warning'))},'AND',{'name': device_type+"_rta_up", 'operator': 'greater_than', 'value': 0 }]})
		
			#TODO: This is a seperate module should be oneto prevent re-looping ovver rules
	for rule in rules:
		if rule in set(service_name_with_operator_in):
			#service_name = "_".join(rule.split("_")[0:4])
			service_rules = rules.get(rule)
			for i in range(1,len(service_rules)+1):		
				severities = service_rules.get("Severity%s"%i)
				for x in range(1,len(severities),2):
					if severities[x].get("name") in operator_name_with_operator_in.keys():
						severities[x]["operator"] = operator_name_with_operator_in.get(severities[x].get("name"))

	return rules

###############################################ETL FUNCTIONS########################################################################################
def get_required_static_data():
	print ("Extracting Data")
	service_threshold_query = Variable.get('q_get_thresholds')
	data = execute_query(service_threshold_query)
	rules_dict = createDict(data)
	Variable.set("rules",str(rules_dict))


def init_etl():
	logging.info("TODO : Check All vars and Airflow ETL Environment here")
	redis_hook_4.flushall("*")
	
	#for db in databases:
	#	print ("clearing sv_%s"%db)OC
	#	memc_con.set("sv_%s"%db,"")
	#	memc_con.set("nw_%s"%db,"")
	#Variable.set('service_memc_key',"")
	#Variable.set('network_memc_key',"")

	logging.info("Flushed all in redis_hook_4 connection")
	logging.info("All Variables Found")	


def get_from_socket(site_name,query,socket_ip,socket_port):
	"""
	Function_name : get_from_socket (collect the query data from the socket)

	Args: site_name (poller on which monitoring data is to be collected)

	Kwargs: query (query for which data to be collectes from nagios.)

	Return : None

	raise
	     Exception: SyntaxError,socket error
    	"""
	#socket_path = "/omd/sites/%s/tmp/run/live" % site_name
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	s.connect((socket_ip, socket_port))
	#s.connect(socket_path)
	s.send(query)
	s.shutdown(socket.SHUT_WR)
	output = ''
	wait_string= ''
	while True:
		try:
			out = s.recv(100000000)
		except socket.timeout,e:
			err=e.args[0]
			print 'socket timeout ..Exiting'
			if err == 'timed out':
				sys.exit(1) 
		out.strip()
		
		if not len(out):
			break;
		output += out

	return output


	#Func to get devices which are down so as not to process those
def get_devices_down(**kwargs):
	#device_down = Variable.get("topology_device_down_query")
	device_down_list = []
	

	try:
		device_down_query = "GET services\nColumns: host_name\nFilter: service_description ~ Check_MK\nFilter: service_state = 3\n"+\
							"And: 2\nOutputFormat: python\n"
	except Exception:
		logging.info("Unable to fetch Network Query Failing Task")
		return 1

	
	site_name = kwargs.get('params').get("site-name")
	site_ip = kwargs.get('params').get("ip")
	site_port = kwargs.get('params').get("port")

	
	logging.info("Extracting data for site "+str(site_name)+"@"+str(site_ip)+" port "+str(site_port))
	topology_data = []
	try:		
		devices_down = eval(get_from_socket(site_name, device_down_query,site_ip,site_port).strip())
		device_down_list =[str(item) for sublist in devices_down for item in sublist]
		
	except Exception:
		logging.error("Unable to get Device Down Data")
		traceback.print_exc()
	try:
		redis_hook_4.rpush("current_down_devices_%s"%site_name,device_down_list)
		return True
	except Exception:
		logging.info("Unable to insert device down data")
		return False
def debug_etl():
	logging.info("Debugging")
	nw_keys = redis_hook_4.get_keys("nw_agg_nocout_*")
	sv_keys = redis_hook_4.get_keys("sv_agg_nocout_*")
	debug_file_path_nw = "/home/tmadmin/nw.debg"
	debug_file_path_sv = "/home/tmadmin/sv.debg"
	with open(debug_file_path_nw,"wb+") as file:
		for key in nw_keys:
			data = redis_hook_4.rget(key)	
		
			for slot in data:
				slot = eval(slot)
				for k,v in enumerate(slot):
					slot[k] = eval (v)

				keys = ['min_value', 'check_time', 'severity', 'service', 'avg_value', 'max_value', 'age', 'local_timestamp', 'site', 'war', 'host', 'machine_name','refer', 'cric', 'ip_address', 'ds', 'cur']
				dict_writer = csv.DictWriter(file, keys,lineterminator=";\r\n",delimiter=",",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
				dict_writer.writeheader()
				try:
					dict_writer.writerows(slot)
				except UnicodeEncodeError:
					continue
		file.close()


	with open(debug_file_path_sv,"wb+") as file:
		for key in sv_keys:
			data = redis_hook_4.rget(key)
		
			for slot in data:
				keys = ['machine_name', 'site', 'cric', 'host', 'ip_address', 'ds', 'cur', 'check_time', 'severity', 'service', 'age', 'local_timestamp','war', 'refer']
				slot = eval(slot)
				for k,v in enumerate(slot):
					slot[k] = eval (v)

				dict_writer = csv.DictWriter(file, keys,lineterminator=";\r\n",delimiter=",",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
				dict_writer.writeheader()
				try:
					dict_writer.writerows(slot)
				except UnicodeEncodeError:
					continue
		file.close()
def get_time():
	return time.time()

def subtract_time(start_time):
	diff_time = time.time() - start_time 
	return int(diff_time)