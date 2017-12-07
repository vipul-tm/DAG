from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.hooks import RedisHook
import logging
import traceback
from etl_tasks_functions import get_from_socket
#TODO: Commenting Optimize
#######################################DAG CONFIG####################################################################################################################

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
    #'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    # 'sla' : timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
PARENT_DAG_NAME = "DEVICE_DOWN"

Q_PUBLIC = "calculation_q"
Q_PRIVATE = "private_q"
Q_OSPF = "poller_queue"



main_device_down_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/20 * * * *',)
config = eval(Variable.get('system_config_o1'))
debug_mode = eval(Variable.get("debug_mode"))
sum=0
#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################




 #Func to get devices which are down so as not to process those
def get_devices_down(**kwargs):
    #device_down = Variable.get("topology_device_down_query")
    all_device_down_list = []
    sum=0
    for machine in config:
        sites=machine.get('sites')
        site_ip=machine.get('ip')
        device_down_list = []
        for site in sites:
            site_port = site.get('port')
            site_name = site.get('name')
            try:
		device_down_query = "GET services\nColumns: host_name\nFilter: service_description ~ Check_MK\nFilter: service_state = 3\nFilter: service_state = 2\nOr: 2\n"+\
                                "And: 2\nOutputFormat: python\n"
            except Exception:
                logging.error("Unable to fetch Network Query Failing Task")
                return 1

            
            
            
            logging.error("Extracting data for site "+str(site_name)+"@"+str(site_ip)+" port "+str(site_port))
            topology_data = []
	    device_down_list = []
            try:        
                devices_down = eval(get_from_socket(site_name, device_down_query,site_ip,site_port).strip())
                device_down_list =[str(item) for sublist in devices_down for item in sublist]
		len_of_data_recieved = len(device_down_list)
		sum=sum+len_of_data_recieved
                logging.error("Recieved Length : %s | Total yet : %s"%(len_of_data_recieved,sum))
                all_device_down_list.extend(device_down_list)
            except Exception:
                logging.error("Unable to get Device Down Data")
                traceback.print_exc()


    try:
        logging.error("Setting data for all sites at Static DB 5 of len %s"%(len(all_device_down_list)))
        redis_hook_5.set("current_down_devices_all",all_device_down_list)
        return True
    except Exception:
        logging.error("Unable to insert device down data")
        return False


device_down_task = PythonOperator(
task_id="find_down_devices_for_all",
provide_context=True,
python_callable=get_devices_down,
dag=main_device_down_dag,
queue = Q_OSPF
)


