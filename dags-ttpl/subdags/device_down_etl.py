from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from etl_tasks_functions import get_from_socket
import logging
import itertools
import traceback
import socket
default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 03, 30,13,00) - timedelta(minutes=5),
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

def device_down_etl(parent_dag_name, child_dag_name, start_date, schedule_interval,celery_queue):
	config = eval(Variable.get('system_config'))

	dag_subdag_device_down = DAG(
    		dag_id="%s.%s" % (parent_dag_name, child_dag_name),
    		schedule_interval=schedule_interval,
    		start_date=start_date,
  		)

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


	for machine in config:
		for site in machine.get('sites'):
			device_down_task = PythonOperator(
			task_id="find_down_devices_%s"%site.get('name'),
			provide_context=True,
			python_callable=get_devices_down,
			params={"ip":machine.get('ip'),"port":site.get('port'),"site-name":site.get('name')},
			dag=dag_subdag_device_down,
			queue = celery_queue
			)	
	return dag_subdag_device_down
