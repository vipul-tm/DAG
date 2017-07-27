from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from airflow.hooks import  MemcacheHook
from subdags.format_utility import get_previous_device_states
from subdags.format_utility import create_device_state_data
from  etl_tasks_functions import get_time
from  etl_tasks_functions import subtract_time
import logging
import itertools
import socket
import random
import traceback
import time
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
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4") #number specifies the DB in Use
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')

def network_etl(parent_dag_name, child_dag_name, start_date, schedule_interval,celery_queue):
	config = eval(Variable.get('system_config'))
	not_has_previous_data = False #variable to check if previous hosts states are saved in memcache or redis  if not found we create it 
	#not_has_previous_data = True #uncomment this file if you want to create the host - device mapping dict
	if get_previous_device_states(redis_hook_5) == None:
		create_device_state_data()	

	dag_subdag = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
    		
  		)
	#TODO: Create hook for using socket with Pool
	#TODO: make this common
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
		machine = site_name[:-8]
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
		
		     	if not len(out):
				break;
		     	output += out

		return output
	
	def extract_and_distribute(**kwargs):
		try:
			network_query = Variable.get('network_query')
			device_slot = Variable.get("device_slot_network")

		except Exception:
			logging.info("Unable to fetch Network Query Failing Task")
			return 1

		task_site = kwargs.get('task_instance_key_str').split('_')[4:7]
		site_name = "_".join(task_site)
		site_ip = kwargs.get('params').get("ip")
		site_port = kwargs.get('params').get("port")
		logging.info("Extracting data for site"+str(site_name)+"@"+str(site_ip)+" port "+str(site_port))

		start_time = float(Variable.get("data_network_extracted_till_%s"%site_name))
		end_time = time.time()
		Variable.set("data_network_extracted_till_%s"%(site_name),end_time)

		network_query = network_query%(start_time,end_time)
		
		network_data = []
		try:
			start_time = get_time()
			network_data = eval(get_from_socket(site_name, network_query,site_ip,site_port))
			logging.info("Time to get Data from Poller is %s"%subtract_time(start_time))
		except Exception:
			logging.error("Unable to get Network Data")
			traceback.print_exc()
		#TODO:
		if len(network_data) > 0: 		
			group_iter = [iter(network_data)]*int(device_slot)
			device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
			logging.info("Got data of length %s and make slots as %s"%(len(network_data),len(device_slot_data)))
			i=1;
			for slot in device_slot_data:	
				redis_hook_4.rpush("nw_"+site_name+"_slot_"+str(i),slot)
				i+=1

			Variable.set("nw_%s_slots"%(site_name),str(len(device_slot_data)))
			if len(device_slot_data) >0:
				return "Success"
			else:
				logging.info("WTF NO Data :O")

		else:
			logging.info("Unable to extract data for time %s to %s "%(start_time,end_time))
			return "No Raw Data"

		

		
			

		

	for machine in config :
		for site in machine.get('sites'):
			PythonOperator(
	   		task_id="Network_extract_%s"%(site.get('name')),
	  		provide_context=True,
	 		python_callable=extract_and_distribute,
			params={"ip":machine.get('ip'),"port":site.get('port')},
			dag=dag_subdag,
			queue = celery_queue
			)
	return 	dag_subdag

