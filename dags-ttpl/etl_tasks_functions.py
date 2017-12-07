from airflow.models import Variable
from airflow.operators import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks import RedisHook
from airflow.hooks import  MemcacheHook
import socket
import logging
import csv #for debugging task
import time
from pprint import pprint
import traceback
SQLhook=MySqlHook(mysql_conn_id='application_db')
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")
#redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
databases = eval(Variable.get('databases'))
#############################################HELPERS################################################



###############################################ETL FUNCTIONS########################################################################################


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

	#####################TEMPORARY####################
def get_bs_all(ip_add):
	bs_list = eval(Variable.get('ml_bs_list'))
	return bs_list

def send_data_to_kafka():
	#nw_keys = redis_hook_4.get_keys("nw_agg_nocout_ospf1")
	#sv_keys = redis_hook_4.get_keys("sv_agg_nocout_ospf1")
	logging.info("Starting Tasks")
	skeleton_dict = {}
	skeleton_dict_bs = {}
	#ip_add = ['10.170.72.17','10.170.72.19','10.170.72.20','10.170.72.48','10.170.72.47','10.170.72.55','10.170.72.60','10.170.72.43','10.170.72.41'] #the ip ss to get data of
	
	ip_add = eval(Variable.get('ml_ss_list'))

	ip_list_ser = []
	ip_list_bs_ser = []
	ip_list = []
	ip_list_bs = []	
	services_list = ['wimax_pmp1_ul_util_bgp','wimax_pmp2_ul_util_bgp','wimax_pmp1_dl_util_bgp','wimax_pmp2_dl_util_bgp','wimax_ul_rssi','wimax_dl_rssi','wimax_ul_cinr','wimax_dl_cinr','wimax_ul_intrf','wimax_dl_intrf','wimax_ss_ul_utilization','wimax_ss_dl_utilization']
	net_list = ['pl','rta']
		

	ip_add_bs = get_bs_all(ip_add)
	bs_ss_mapper = {}
	for i,bs in enumerate(ip_add_bs):
		if bs in bs_ss_mapper.keys():
			bs_ss_mapper[bs].append(ip_add[i])
		else:
			bs_ss_mapper[bs] = [ip_add[i]]
	print bs_ss_mapper

	data = redis_hook_4.rget('nw_agg_nocout_ospf1')
	st = time.time()
	for slot in data:
		slot = eval(slot)
		for services in slot:
			services =  eval(services)
			try:					
				cur_ip = services.get('ip_address')
				
				if cur_ip in ip_add:
					#logging.info("Got IP %s"%(cur_ip))
					ip_address = services.get('ip_address')
					skeleton_dict['ip_address'] = str(ip_address)
					skeleton_dict['service'] = str(services.get('ds'))
					skeleton_dict['cur'] = services.get('cur')
					skeleton_dict['host'] = services.get('host')
					ip_list.append(skeleton_dict.copy())
			
				if cur_ip in ip_add_bs:

					logging.info("Got %s %s"%(cur_ip,services.get('cur')))
					bs_ip_address = str(services.get('ip_address'))	
					
					for ss_ip in bs_ss_mapper.get(bs_ip_address):						
						skeleton_dict_bs['bs_ip_address'] = str(bs_ip_address)
						skeleton_dict_bs['ss_ip_address'] = ss_ip
						skeleton_dict_bs['service'] = str(services.get('ds'))
						skeleton_dict_bs['cur'] = services.get('cur')
						skeleton_dict['host'] = services.get('host')
						ip_list_bs.append(skeleton_dict_bs.copy())
				else:
					#print "Ignoring %s"%(cur_ip)
					pass
			except Exception:
				continue

	logging.info("%s Seconds Loop 1 done" %(time.time() - st))
	
	st = time.time()
	sv_pre_data=[]

	data_sv = redis_hook_4.rget('sv_agg_nocout_ospf1')
	#print "SLotDAAAAAAAAAAAAAAAAAAAAAAAAA"
	#print data_sv
	for slots in data_sv:
		print "slot"
		slots = eval(slots)
		print 22
		print len(slots)
		for services in slots:
			#print 44
			services = eval(services)
			try:
				#print services
				sv_pre_data.append(services.copy())
					
			except Exception:
				continue
				traceback.print_exc()

	logging.info("Step 2")
	for services in sv_pre_data:
		cur_ip = services.get('ip_address')
		if cur_ip in ip_add:
			#print 2,services.get('service')
                        ip_address = services.get('ip_address')
                        skeleton_dict['ip_address'] = str(ip_address)
                        skeleton_dict['service'] = str(services.get('service'))
                        skeleton_dict['cur'] = services.get('cur')
                        skeleton_dict['host'] = services.get('host')
                        ip_list_ser.append(skeleton_dict.copy())

                if cur_ip in ip_add_bs:
                        bs_ip_address = services.get('ip_address')
                        ss_ip = ip_add_bs.index(bs_ip_address)
                        for ss_ip in bs_ss_mapper.get(bs_ip_address):	
	                        skeleton_dict_bs['bs_ip_address'] = str(bs_ip_address)
	                        skeleton_dict_bs['ss_ip_address'] = ss_ip
	                        skeleton_dict_bs['service'] = str(services.get('service'))
	                        skeleton_dict_bs['cur'] = services.get('cur')
	                        skeleton_dict['host'] = services.get('host')
	                        ip_list_bs_ser.append(skeleton_dict_bs.copy())		



	logging.info("%s Seconds Loop 2 done" %(time.time() - st))
	
	st = time.time()

	all_data_dict = {}
	print ip_list_bs
	for x in ip_list :
		if x.get('service') in net_list:
			if x.get('ip_address') not in all_data_dict.keys():
				all_data_dict[x.get('ip_address')] = {

				"ss_%s"%(x.get('service')):x.get('cur'),
				"ip_address":x.get('ip_address'),
				"host":x.get('host')
				}
			else:
				all_data_dict.get(x.get('ip_address')).update({"ss_%s"%(x.get('service')):x.get('cur')})


	for x in ip_list_bs:
		
		if x.get('service') in net_list:
			if x.get('ss_ip_address') not in all_data_dict.keys():
				all_data_dict[x.get('ss_ip_address')] = {

				'bs_%s'%(x.get('service')):x.get('cur')

				}
			else:
				#logging.info("Updating for service -- > %s %s %s" %(x.get('service'),x.get('cur'),x.get('ss_ip_address')))
				all_data_dict.get(x.get('ss_ip_address')).update({"bs_%s"%(x.get('service')):x.get('cur')})
		else:
			logging.info("Skipping for service -- > %s " %(x.get('service')))		


	for x in ip_list_ser :
		if x.get('service') in services_list:
			if x.get('ip_address') not in all_data_dict.keys():
				
				all_data_dict[x.get('ip_address')] = {
				"%s"%(x.get('service')):x.get('cur')
				}
			else:
				
				all_data_dict.get(x.get('ip_address')).update({"%s"%(x.get('service')):x.get('cur')})


	for x in ip_list_bs_ser :
		if x.get('service') in services_list:
			
			if x.get('ss_ip_address') not in all_data_dict.keys():
				
				all_data_dict[x.get('ss_ip_address')] = {
				'%s'%(x.get('service')):x.get('cur')
				}
			else:
				
				all_data_dict.get(x.get('ss_ip_address')).update({"%s"%(x.get('service')):x.get('cur')})


	
	for ss_ip in all_data_dict:
		dl_intrf = all_data_dict.get(ss_ip).get('wimax_dl_intrf')
		ul_intrf = all_data_dict.get(ss_ip).get('wimax_ul_intrf')
		all_data_dict.get(ss_ip)['wimax_dl_intrf'] = 0 if dl_intrf == 'Norm' else (1 if dl_intrf == 'War' else 2)
		all_data_dict.get(ss_ip)['wimax_ul_intrf'] = 0 if ul_intrf == 'Norm' else (1 if ul_intrf == 'War' else 2)

	logging.info("%s Seconds Loop 3 done" %(time.time() - st))
	

	final_data = []
	print all_data_dict
	for ss_ip in  all_data_dict:
		logging.info("We are creating")
		ss_ip = all_data_dict.get(ss_ip)
		final_data.append((ss_ip.get('bs_pl'),
			ss_ip.get('bs_rta'),
			ss_ip.get('wimax_pmp1_ul_util_bgp'),
			ss_ip.get('wimax_pmp2_ul_util_bgp'),
			ss_ip.get('wimax_pmp1_dl_util_bgp'),
			ss_ip.get('wimax_pmp2_dl_util_bgp'),
			ss_ip.get('ss_pl'),
			ss_ip.get('ss_rta'),
			ss_ip.get('wimax_ul_rssi'),
			ss_ip.get('wimax_dl_rssi'),
			ss_ip.get('wimax_ul_cinr'),
			ss_ip.get('wimax_dl_cinr'),
			ss_ip.get('wimax_ul_intrf'),
			ss_ip.get('wimax_dl_intrf'),
			str(ss_ip.get('wimax_ss_ul_utilization')),
			str(ss_ip.get('wimax_ss_dl_utilization')),
			ss_ip.get('ip_address'),
			ss_ip.get('host'),		
			))
		logging.info("we are done")
		
	try:
		logging.info("Successfully pushed data of length %s %s"%(len(final_data) , final_data))
		redis_hook_4.rpush("ml_queue_main",final_data)
	except Exception,e:
		logging.info("Error in producing data")
		print e
