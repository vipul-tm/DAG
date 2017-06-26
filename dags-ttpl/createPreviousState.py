from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks import RedisHook
from shutil import copyfile
import logging

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "GETSTATES"

prev_state_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once')
config = eval(Variable.get('system_config'))
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")

def create_prev_state(**kwargs):
    
    
    #key = ospf1_slave_1_last_pl_info
    data = {}
    for conn_id in [1,2,3,4,5]:
        redis_hook = RedisHook(redis_conn_id="redis_prev_state_%s"%conn_id)
        for ds in ['pl','rta']:
            for site in [1,2,3,4,5,6,7,8]:
                data_redis = redis_hook.hgetall("ospf%s_slave_%s_last_%s_info"%(conn_id,site,ds))
                key = "ospf%s_slave_%s_%s"%(conn_id,site,ds)
                data[key] = data_redis

    machine_state_list_pl = {}
    machine_state_list_rta = {}
    host_mapping = {}
##########################################################################################
    logging.info("Creating IP to Host Mapping from HOST to IP mapping")
    ip_mapping = get_ip_host_mapping()
    for host_name,ip in ip_mapping.iteritems():
        host_mapping[ip] = host_name

    logging.info("Mapping Completed for %s hosts"%len(host_mapping))
    ######################################33###################################################
    for key in data:
        site_data = data.get(key)
        for device in site_data:
            host = host_mapping.get(device)
            if "pl" in key: 
                machine_state_list_pl[host] = {'state':eval(site_data.get(device))[0],'since':eval(site_data.get(device))[1]}
            elif "rta" in key:
                machine_state_list_rta[host] = {'state':eval(site_data.get(device))[0],'since':eval(site_data.get(device))[1]}

    print len(machine_state_list_pl),len(machine_state_list_rta)

    main_redis_key = "all_devices_state"
    rta = "all_devices_state_rta"
    redis_hook_5.set(main_redis_key,str(machine_state_list_pl))
    redis_hook_5.set(rta,str(machine_state_list_rta))

def get_ip_host_mapping():
    path = Variable.get("hosts_mk_path")
    try:
        host_var = load_file(path)
        ipaddresses = host_var.get('ipaddresses')
        return ipaddresses 
    except IOError:
        logging.error("File Name not correct")
        return None
    except Exception:
        logging.error("Please check the HostMK file exists on the path provided ")
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




redis_copy = PythonOperator(
    task_id="create_prev_state_for_all",
    provide_context=False,
    python_callable=create_prev_state,
    dag=prev_state_dag
    )
