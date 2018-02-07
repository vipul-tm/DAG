from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.hooks import RedisHook
from airflow.hooks import MemcacheHook
import logging
import traceback
from etl_tasks_functions import get_from_socket
from sync import get_host_ip_mapping
import sys
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
memc = MemcacheHook(memc_cnx_id = 'memc_cnx')
PARENT_DAG_NAME = "DEVICE_ACTIVE"

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



def get_hostname_mac_mapping():
    path= Variable.get("hosts_mk_path")
    host_mac_mapper = {}
    try:
        host_var = load_file(path)
        allhosts = host_var.get('all_hosts')
        for host in  allhosts:
	    host_name = host.split("|")[0]
	    mac = host.split("|")[2]
	    host_mac_mapper[mac] = host_name
	return host_mac_mapper
    except IOError:
        logging.error("File Name not correct")
        return None
    except Exception:
        logging.error("Please check the HostMK file exists on the path provided ")
	traceback.print_exc()	
        return None

def load_file(file_path):
        #Reset the global vars
        host_vars = {
                "all_hosts": [],
                "ipaddresses": {},
                "host_attributes": {},
                "host_contactgroups": [],
        }
	try:
            	execfile(file_path, host_vars, host_vars)
                del host_vars['__builtins__']
        except IOError, e:
                pass
        return host_vars

 #Func to get devices which are down so as not to process those
def get_active_devices(**kwargs):
    #device_down = Variable.get("topology_device_down_query")
    mac_to_hostname_mapping = get_hostname_mac_mapping()
    host_mk_dict = eval(Variable.get("hostmk.dict"))
    #down_devices = eval(redis_hook_5.set("current_down_devices_all"))
    all_device_dict_bs={'CanopyPM100AP':[],'Cambium450iAP':[],'Cambium450mAP':[]} 
    all_device_dict_ss = {'CanopySM100SS':[],'Cambium450iSM':[],'Cambium450mSM':[]}
    ip_host_mapping = get_host_ip_mapping()
    all_reg_dict = {}
    all_pl_dict = {}
    all_conn_ss_dict = {}
    all_conn_ss_dict_canopy = {}
    all_active_device_list = [] #contains hostname of the active devices
    all_active_device_list_ip = [] #contains ip of the active devices
    sum=0
    counter_dev = 0 
    counter_dev_bs = 0
    try:        
        for device in host_mk_dict.keys():
            if host_mk_dict.get(device) in ['CanopySM100SS','Cambium450iSM','Cambium450mSM']:
                all_device_dict_ss[host_mk_dict.get(device)].append(device)
            elif host_mk_dict.get(device) in ['CanopyPM100AP','Cambium450iAP','Cambium450mAP']:
                all_device_dict_bs[host_mk_dict.get(device)].append(device)

	for device in all_device_dict_bs.get('CanopyPM100AP'):
            ip_address = ip_host_mapping.get(device)
            all_conn_ss_dict_canopy[device] = memc.get("%s_conn_ss"%(device))
            counter_dev_bs+=1
        for device in all_device_dict_bs.get('Cambium450iAP'):
            all_conn_ss_dict[device] = memc.get("%s_conn_ss"%(device))
            counter_dev_bs+=1
        for device in all_device_dict_bs.get('Cambium450mAP'):
            all_conn_ss_dict[device] = memc.get("%s_conn_ss"%(device))
            counter_dev_bs+=1




	for device in all_device_dict_ss.get('CanopySM100SS'):
	    ip_address = ip_host_mapping.get(device)
            all_reg_dict[device] = memc.get("%s_cambium_reg_count"%(ip_address))
	    all_pl_dict[device] = memc.get("%s_ping_list"%(ip_address))
	    counter_dev+=1
	for device in all_device_dict_ss.get('Cambium450iSM'):
            ip_address = ip_host_mapping.get(device)
            all_reg_dict[device] = memc.get("%s_cam450i_ss_reg_count"%(ip_address))
	    all_pl_dict[device] = memc.get("%s_ping_list"%(ip_address))
	    counter_dev+=1
	for device in all_device_dict_ss.get('Cambium450mSM'):
            ip_address = ip_host_mapping.get(device)
            all_reg_dict[device] = memc.get("%s_cam450m_ss_reg_count"%(ip_address))
	    all_pl_dict[device] = memc.get("%s_ping_list"%(ip_address))
	    counter_dev+=1
	for dev in all_pl_dict.keys():
	    dev_reg = all_reg_dict.get(dev)
	    dev_pl = all_pl_dict.get(dev)
	    if dev_pl and dev_reg:
		if dev_pl[0] == 100 and  dev_pl[1] == 100  and  dev_pl[2] == 100 and dev_reg.count(dev_reg[0]) == len(dev_reg):
		    logging.info("Device is Down")
		    
		else:

		    logging.info("Device is up")
		    all_active_device_list.append(dev)
		    try:
		        all_active_device_list_ip.append(ip_host_mapping.get(dev))
		    except Exception:
			logging.warning("ip not found for device %s"%(dev))
	    else:
		logging.info("Device is down")
        #logging.error("Recieved Length : %s | Total yet : %s"%(len_of_data_recieved,sum))
	for bs in  all_conn_ss_dict.keys():
	    #we will get mac address of the connected ss here
	    active_ss_list = []
	    try:
		conn_ss_mac = all_conn_ss_dict.get(bs)
		if conn_ss_mac:
		    for mac in conn_ss_mac:
		        conn_ss_hostname=mac_to_hostname_mapping.get(mac)
		        if conn_ss_hostname in all_active_device_list:
			    active_ss_list.append(conn_ss_hostname)

		#print "%s_%s = %s"%(bs,'active_ss',active_ss_list)    
		memc.set("%s_active_ss"%(bs),active_ss_list)
	    except Exception:
		print "Error While Setting active devices in memc"
		traceback.print_exc()
		
	
	for bs in  all_conn_ss_dict_canopy.keys():
            #we will get ip address of the connected ss here
            active_ss_list = []
            try:
                conn_ss_ip = all_conn_ss_dict_canopy.get(bs)
                if conn_ss_ip:
                    for ip in conn_ss_ip:
                        if ip in all_active_device_list_ip:
                            active_ss_list.append(ip)

                #print "%s_%s = %s"%(bs,'active_ss',active_ss_list)
		memc.set("%s_active_ss"%(bs),active_ss_list)
            except Exception:
                print "Error While Setting active devices in memc"
                traceback.print_exc()


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
task_id="find_active_devices_for_all",
provide_context=True,
python_callable=get_active_devices,
dag=main_device_down_dag,
queue = Q_OSPF
)






