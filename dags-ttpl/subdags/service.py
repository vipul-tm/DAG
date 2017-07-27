from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
import logging
import itertools
import traceback
import socket
from  etl_tasks_functions import get_time
from  etl_tasks_functions import subtract_time
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

OKGREEN = '\033[92m'
NC='\033[0m'
redis_hook_4 = RedisHook(redis_conn_id="redis_hook_4")

def service_etl(parent_dag_name, child_dag_name, start_date, schedule_interval,celery_queue):
	config = eval(Variable.get('system_config'))

	dag_subdag = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
    		
  		)

	#TODO: Create hook for using socket with Pool
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

	def extract_and_distribute(*args,**kwargs):
		st = get_time()

		try:
			service_query = Variable.get('service_query') #to get LQL to extract service
			device_slot = Variable.get("device_slot_service") #the number of devices to be made into 1 slot
			site_ip = kwargs.get('params').get("ip")
			site_port = kwargs.get('params').get("port")
		except ValueError:
			logging.info("Unable to fetch Service Query Failing Task")
			return 1

		task_site = kwargs.get('task_instance_key_str').split('_')[4:7]
		site_name = "_".join(task_site)

		start_time = float(Variable.get("data_service_extracted_till_%s"%site_name))
		
		end_time = time.time()

		service_query =  "GET services\nColumns: host_name host_address service_description service_state "+\
                            "last_check service_last_state_change host_state service_perf_data\nFilter: service_description ~ _invent\n"+\
                            "Filter: service_description ~ _status\n"+\
                            "Filter: service_description ~ Check_MK\n"+\
                            "Filter: service_description ~ PING\n"+\
                            "Filter: service_description ~ .*_kpi\n"+\
                            "Filter: service_description ~ wimax_ss_port_params\n"+\
                            "Filter: service_description ~ wimax_bs_ss_params\n"+\
                            "Filter: service_description ~ wimax_aggregate_bs_params\n"+\
                            "Filter: service_description ~ wimax_bs_ss_vlantag\n"+\
                            "Filter: service_description ~ wimax_topology\n"+\
                            "Filter: service_description ~ cambium_topology_discover\n"+\
                            "Filter: service_description ~ mrotek_port_values\n"+\
                            "Filter: service_description ~ rici_port_values\n"+\
                            "Filter: service_description ~ rad5k_topology_discover\n"+\
                            "Or: 14\nNegate:\n"+\
                            "Filter: last_check >= %s\n" % start_time+\
                         	"Filter: last_check < %s\n" % end_time+\
                            "OutputFormat: python\n"

		try:
			start_time = get_time()
			service_data = eval(get_from_socket(site_name, service_query,site_ip,site_port))
			logging.info("Fetch Time %s"%subtract_time(start_time))
			Variable.set("data_service_extracted_till_%s"%site_name,end_time)
			#for x in service_data:
			#	 if x[1] == '10.175.161.2':
			#		print x
		except Exception:
			logging.error(OKGREEN+"Unable to fetch the data from Socket")
			logging.error('SITE:'+str(site_name)+"\n PORT : "+str(site_port )+ "\n IP: " +str(site_ip)+NC)
			service_data = []
			traceback.print_exc()
		if len(service_data) > 0:

			logging.info("The length of Data recieved " + str(len(service_data)))
			group_iter = [iter(service_data)]*int(device_slot)
			device_slot_data = list(([e for e in t if e !=None] for t in itertools.izip_longest(*group_iter)))
			i=1;
			logging.info("Service Slot created in redis -> " + str(len(device_slot_data)))		
			for slot in device_slot_data:
				redis_hook_4.rpush("sv_"+site_name+"_slot_"+str(i),slot)
				logging.info("Pushing %s"%("sv_"+site_name+"_slot_"+str(i)))
				i+=1
			Variable.set("sv_%s_slots"%(site_name),str(len(device_slot_data)))
			logging.info("Total Time %s"%subtract_time(st))
		else:
			logging.info("Unable to extract data for time %s to %s "%(start_time,end_time))

	for machine in config:
		for site in machine.get('sites'):
			PythonOperator(
	   		task_id="Service_extract_%s"%(site.get('name')),
	  		provide_context=True,
	 		python_callable=extract_and_distribute,
			params={"ip":machine.get('ip'),"port":site.get('port')},
			dag=dag_subdag,
			queue = celery_queue
			)	
	return dag_subdag
