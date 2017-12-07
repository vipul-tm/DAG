from airflow import DAG
from airflow.operators import PythonOperator
from airflow.hooks import RedisHook
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.hooks import RedisHook
from shutil import copyfile
import logging
import traceback
from airflow.hooks import  MemcacheHook

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
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_2")

memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
vrfprv_memc_con  = MemcacheHook(memc_cnx_id = 'vrfprv_memc_cnx')
pub_memc_con  = MemcacheHook(memc_cnx_id = 'pub_memc_cnx')
def create_prev_state(**kwargs):
    
    
    #key = ospf1_slave_1_last_pl_info
    data = {}
    data_down = {}
    for conn_id in [1,2,3,4,5,6,7]:
        redis_hook = RedisHook(redis_conn_id="redis_prev_state_%s"%conn_id)
        if conn_id <= 5:
            for site in [1,2,3,4,5,6,7,8]:
                data_redis_down = redis_hook.hgetall("ospf%s_slave_%s_device_down"%(conn_id,site))
                key = "ospf%s_slave_%s_down"%(conn_id,site)
                data_down[key] = data_redis_down
        elif conn_id == 6:
            for site in [1,2,3,4,5,6]:
                data_redis_prv_down = redis_hook.hgetall("vrfprv_slave_%s_device_down"%(site))
                key = "ospf%s_slave_%s_down"%(conn_id,site)
                data_down[key] = data_redis_prv_down
        elif conn_id == 7:
            for site in [1]:
                data_redis_pub_down = redis_hook.hgetall("pub_slave_%s_device_down"%(site))
                key = "ospf%s_slave_%s_down"%(conn_id,site)
                data_down[key] = data_redis_pub_down

        for ds in ['pl','rta']:
            if conn_id <= 5:
                for site in [1,2,3,4,5,6,7,8]:
                    data_redis = redis_hook.hgetall("ospf%s_slave_%s_last_%s_info"%(conn_id,site,ds))
                    key = "ospf%s_slave_%s_%s"%(conn_id,site,ds)
                    data[key] = data_redis
            elif conn_id == 6:
                for site in [1,2,3,4,5,6]:
                    data_redis_prv = redis_hook.hgetall("vrfprv_slave_%s_last_%s_info"%(site,ds))
                    key = "ospf%s_slave_%s_%s"%(conn_id,site,ds)
                    data[key] = data_redis_prv
            elif conn_id == 7:
                for site in [1]:
                    data_redis_pub = redis_hook.hgetall("pub_slave_%s_last_%s_info"%(site,ds))
                    key = "ospf%s_slave_%s_%s"%(conn_id,site,ds)
                    data[key] = data_redis_pub
                
    machine_state_list_pl = {}
    machine_state_list_rta = {}
    machine_state_list_down = {}
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
        #logging.info("FOR  %s is %s"%(key,len(key)))
        for device in site_data:
            host = host_mapping.get(device)
            if "pl" in key: 
                machine_state_list_pl[host] = {'state':eval(site_data.get(device))[0],'since':eval(site_data.get(device))[1]}
            elif "rta" in key:
                machine_state_list_rta[host] = {'state':eval(site_data.get(device))[0],'since':eval(site_data.get(device))[1]}

    i=0
    for key in data_down:

        site_data_down = data_down.get(key)
        #print "%s ===== %s"%(key,len(site_data_down))
        #logging.info("FOR  %s is %s"%(key,len(key)))
        for device in site_data_down:

            if site_data_down.get(device) != None and site_data_down.get(device) != {} :
                try: 
                    machine_state_list_down[device] = {'state':eval(site_data_down.get(device))[0],'since':eval(site_data_down.get(device))[1]}
                except Exception:
                    pass
                    
                    #logging.info("Device not found in the ")
                    #print site_data_down.get(device)
                    #traceback.print_exc()
            else:
                logging.info("Data not present for device %s "%(host))


    logging.info("Total rejected : %s"%(i))
   # print data_down
    print len(machine_state_list_pl),len(machine_state_list_rta)

    main_redis_key = "all_devices_state"
    rta = "all_devices_state_rta"
    down_key = "all_devices_down_state"

    redis_hook_5.set(main_redis_key,str(machine_state_list_pl))
    redis_hook_5.set(rta,str(machine_state_list_rta))
    redis_hook_5.set(down_key,str(machine_state_list_down))
    logging.info("3 keys generated in redis")
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



def create_ul_issue_kpi_prev_state():

    all_devices = eval(Variable.get("hostmk.dict"))
    services_mapping = eval(Variable.get("ul_issue_kpi_to_formula_mapping"))
    all_services = []
    new_prev_states_dict = {}
    all_device_type = services_mapping.keys()
    for device_type in services_mapping:
        all_services.extend(services_mapping.get(device_type))
        new_prev_states_dict["kpi_ul_prev_state_%s"%(device_type)] = {}

    none_count =0
    for device in all_devices:
        hostname = device
        device_type = all_devices.get(device)
        device_dict={}

        if device_type in all_device_type:
            device_dict[hostname]  = {'state':'unknown','since':'unknown'}
            service = services_mapping.get(device_type)[0]
            kpi_key = "util:%s:%s"%(hostname,service)
            
            try:
                old_states =memc_con.get(kpi_key)
                if old_states == None:
                    old_states = vrfprv_memc_con.get(kpi_key)
                if old_states == None:
                    old_states = pub_memc_con.get(kpi_key)


                if old_states != None:
                    old_severity = old_states.split(",")[0]
                    old_severity_since = old_states.split(",")[1]
                    device_dict[hostname]= {'state':old_severity,'since':old_severity_since}
                    new_prev_states_dict.get("kpi_ul_prev_state_%s"%(device_type))[hostname] = {'state':old_severity,'since':old_severity_since}
                else:
                    #print "None for %s %s"%(kpi_key,old_states)
                    none_count = none_count+1
            except Exception,e:
                print "Unable to get UTIL for %s - %s"%(device_type,e)
                break

    print len(new_prev_states_dict),new_prev_states_dict.keys()
    count_total = 0
    for d in new_prev_states_dict:
        print len(new_prev_states_dict.get(d))
        count_total = count_total + len(new_prev_states_dict.get(d))

    print "None in Memc for %s Devices Total States Found %s"%(none_count,count_total)

    for key in new_prev_states_dict.keys():
        try:
            redis_hook_5.set(key,str(new_prev_states_dict.get(key)))
            logging.info("Setting for Key %s is successful"%(key))
        except Exception:
            logging.error("Unable to add %s key in redis"%(key))



def create_provis_kpi_prev_state():

    all_devices = eval(Variable.get("hostmk.dict"))
    services_mapping = eval(Variable.get("provision_kpi_to_formula_mapping"))
    all_services = []
    new_prev_states_dict = {}
    all_device_type = services_mapping.keys()
    for device_type in services_mapping:
        all_services.extend(services_mapping.get(device_type))
        new_prev_states_dict["kpi_provis_prev_state_%s"%(device_type)] = {}

    none_count =0
    for device in all_devices:
        hostname = device
        device_type = all_devices.get(device)
        device_dict={}

        if device_type in all_device_type:
            device_dict[hostname]  = {'state':'unknown','since':'unknown'}
            service = services_mapping.get(device_type)[0]
            kpi_key = "util:%s:%s"%(hostname,service)
            
            try:
                old_states =memc_con.get(kpi_key)
                if old_states == None:
                    old_states = vrfprv_memc_con.get(kpi_key)
                if old_states == None:
                    old_states = pub_memc_con.get(kpi_key)

                if old_states != None:
                    old_severity = old_states.split(",")[0]
                    old_severity_since = old_states.split(",")[1]
                    device_dict[hostname]= {'state':old_severity,'since':old_severity_since}
                    new_prev_states_dict.get("kpi_provis_prev_state_%s"%(device_type))[hostname]={'state':old_severity,'since':old_severity_since}
                else:
                    #print "None for %s %s"%(kpi_key,old_states)
                    none_count = none_count+1
            except Exception,e:
                print "Unable to get UTIL for %s - %s"%(device_type,e)
                break

    print len(new_prev_states_dict),new_prev_states_dict.keys()
    count_total = 0
    for d in new_prev_states_dict:
        print len(new_prev_states_dict.get(d))
        count_total = count_total + len(new_prev_states_dict.get(d))

    print "None in Memc for %s Devices Total States Found %s"%(none_count,count_total)

    for key in new_prev_states_dict.keys():
        try:
            redis_hook_5.set(key,str(new_prev_states_dict.get(key)))
            logging.info("Setting for Key %s is successful"%(key))
        except Exception:
            logging.error("Unable to add %s key in redis"%(key))

def create_utilization_kpi_prev_state():

    all_devices = eval(Variable.get("hostmk.dict"))
    services_mapping = eval(Variable.get("utilization_kpi_service_mapping"))
    #services_mapping = eval(Variable.get("back_util"))
    all_services = []
    new_prev_states_dict = {}
    all_device_type = services_mapping.keys()
    for device_type in services_mapping:
        all_services.extend(services_mapping.get(device_type))
        new_prev_states_dict["kpi_util_prev_state_%s"%(device_type)] = {}

    none_count =0
    for device in all_devices:
        hostname = device
        device_type = all_devices.get(device)
        device_dict={}

        if device_type in all_device_type:
            device_dict[hostname]  = {'state':'unknown','since':'unknown'}
            services = services_mapping.get(device_type)
            for service in services:
                kpi_key = "util:%s:%s"%(hostname,service)
                prev_dict_key = "%s_%s"%(hostname,service)
                try:
                    old_states =memc_con.get(kpi_key)
                    
                    if old_states == None:
                        old_states = vrfprv_memc_con.get(kpi_key)
                    if old_states == None:
                        old_states = pub_memc_con.get(kpi_key)

                    if old_states != None:
                        old_severity = old_states.split(",")[0]
                        old_severity_since = old_states.split(",")[1]
                        device_dict[hostname]= {'state':old_severity,'since':old_severity_since}

                        new_prev_states_dict.get("kpi_util_prev_state_%s"%(device_type))[prev_dict_key]={'state':old_severity,'since':old_severity_since}
                    else:
                        #print "None for %s %s"%(kpi_key,old_states)
                        none_count = none_count+1
                except Exception,e:
                    print "Unable to get UTIL for %s - %s"%(device_type,e)
                    break

    print len(new_prev_states_dict),new_prev_states_dict.keys()
    count_total = 0
    for d in new_prev_states_dict:
        print len(new_prev_states_dict.get(d))
        count_total = count_total + len(new_prev_states_dict.get(d))

    print "None in Memc for %s Devices Total States Found %s"%(none_count,count_total)

    for key in new_prev_states_dict.keys():
        try:
            redis_hook_5.set(key,str(new_prev_states_dict.get(key)))
            logging.info("Setting for Key %s is successful"%(key))
        except Exception:
            logging.error("Unable to add %s key in redis"%(key))





redis_copy = PythonOperator(
    task_id="create_prev_state_for_all",
    provide_context=False,
    python_callable=create_prev_state,
    dag=prev_state_dag
    )

get_kpi_prev_states_task= PythonOperator(
    task_id="create_ul_issue_kpi_prev_state",
    provide_context=False,
    python_callable=create_ul_issue_kpi_prev_state,
    dag=prev_state_dag
    )

get_provis_kpi_prev_states_task= PythonOperator(
    task_id="create_provision_kpi_prev_state",
    provide_context=False,
    python_callable=create_provis_kpi_prev_state,
    dag=prev_state_dag
    )
get_utilization_kpi_prev_states_task= PythonOperator(
    task_id="create_utilization_kpi_prev_state",
    provide_context=False,
    python_callable=create_utilization_kpi_prev_state,
    dag=prev_state_dag
    )
