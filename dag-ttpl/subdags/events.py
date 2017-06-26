from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.operators import PythonOperator
from airflow.operators import MemcToMySqlOperator
from airflow.hooks import RedisHook
from airflow.models import Variable
from subdags.format_utility import get_threshold
from subdags.format_utility import get_device_type_from_name
from subdags.format_utility import get_previous_device_states
from subdags.events_utility import get_device_alarm_tuple
from subdags.events_utility import update_device_state_values
import logging
import traceback
import tempfile
import os
from os import listdir
from os.path import isfile, join
from airflow.hooks import  MemcacheHook
import time
import math
#TODO: Create operator changed from previous 

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
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
def calculate_events(parent_dag_name, child_dag_name, start_date, schedule_interval):
    config = eval(Variable.get('system_config'))
    memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
    event_rules = eval(Variable.get('event_rules'))
    nw_result_keys = eval(Variable.get("network_memc_key"))
    sv_result_keys = eval(Variable.get("service_memc_key"))
    redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
    redis_hook_network_alarms = RedisHook(redis_conn_id="redis_hook_network_alarms")
    all_devices_states = get_previous_device_states(memc_con,redis_hook_5)
    
    events_subdag = DAG(
            dag_id="%s.%s" % (parent_dag_name, child_dag_name),
            schedule_interval=schedule_interval,
            start_date=start_date,
        )

    def extract_and_distribute_nw(**kwargs):
        print("Finding Events for network")
        memc_key=kwargs.get('params').get('memc_key')
        network_data = memc_con.get(memc_key)
        if len(network_data) > 0:
            all_pl_rta_trap_list= get_device_alarm_tuple(network_data,all_devices_states,event_rules)
            if len(all_pl_rta_trap_list) > 0:
                machine_name = memc_key.split("_")[1]
                redis_key = 'queue:network:snmptt:%s' % machine_name
                try:
                    redis_hook_network_alarms.rpush(redis_key,all_pl_rta_trap_list)
                    
                except Exception:
                    logging.error("Unable to insert data to redis.")
            else:
                logging.info("No Traps recieved") 
        else:
            logging.info("No Data Found in memC")

    def extract_and_distribute_sv(**kwargs):
        print("Finding Events for service")

    #TODO: We can parallelize this operator as runnning for only machine
    update_refer = PythonOperator(
            task_id="update_redis_refer",
            provide_context=False,
            python_callable=update_device_state_values,
            dag=events_subdag
            )

    if len(nw_result_keys) > 0:
        for key in nw_result_keys :
            slot = "_".join(key.split("_")[1:6])
            event_nw = PythonOperator(
            task_id="discover_events_nw_%s_"%(slot),
            provide_context=True,
            python_callable=extract_and_distribute_nw,
            params={"memc_key":key},
            dag=events_subdag
            )
            event_nw >> update_refer

    if len(sv_result_keys) > 0:
        for sv_keys in sv_result_keys:
            slot = "_".join(sv_keys.split("_")[1:6])
            event_sv = PythonOperator(
            task_id="discover_events_sv_%s"%(slot),
            provide_context=True,
            python_callable=extract_and_distribute_sv,
            params={"memc_key":sv_keys},
            dag=events_subdag
            )
            event_sv >> update_refer
    
    return  events_subdag



