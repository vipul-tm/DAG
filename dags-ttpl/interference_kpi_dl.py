from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from subdags.interference_kpi_dl_subdag import process_interference_dl_kpi
from collections import defaultdict
from airflow.hooks import RedisHook
#from etl_tasks_functions import get_required_static_data
#from etl_tasks_functions import init_etl
#from etl_tasks_functions import debug_etl
#from etl_tasks_functions import send_data_to_kafka
#from airflow.operators import ExternalTaskSensor
#from airflow.operators import MemcToMySqlOperator
#from celery.signals import task_prerun, task_postrun
#from airflow.operators.kafka_extractor_operator import KafkaExtractorOperator
import logging
import traceback
default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(minutes=5),
    #'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    #'catchup': False,
    'provide_context': True,
    # 'sla' : timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}


Q_PUBLIC = "poller_queue"
Q_PRIVATE = "formatting_queue"
Q_OSPF = "poller_queue"
Q_PING = "poller_queue"

PARENT_DAG_NAME = "INTERFERENCE_KPI_DL"
#3-59/5 * * * *
# 3,8,13,18,23,28,33,38,43,48,53,58 * * * *
interference_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/20 * * * *')
redis_hook_12 = RedisHook(redis_conn_id="redis_hook_12")
redis_hook_2 = RedisHook(redis_conn_id="redis_hook_2")
technologies = eval(Variable.get('interference_kpi_technologies'))
machines = eval(Variable.get("system_config_o1"))
devices = eval(Variable.get('hostmk.dict.site_mapping'))
all_sites=[]
def init_kpi():
    logging.info("TODO : Check All vars and Airflow ETL Environment here")
    redis_hook_12.flushall("*")
    logging.info("Flushed all in redis_hook_12 connection")

def get_previous_device_states(device_type):
    prev_state = eval(redis_hook_2.get("kpi_ul_prev_state_%s"%device_type))
    return prev_state

def update_age(**kwargs):
    try:

        device_type = kwargs.get('params').get('device_type')
        bs_or_ss = kwargs.get('params').get('type')
        old_states = get_previous_device_states(device_type) #get PL here           

        aggregated_data_vals = list(redis_hook_12.get_keys("aggregated_interference_*_%s_%s"%(bs_or_ss,device_type)))

        for key in aggregated_data_vals:
            data = eval(redis_hook_12.get(key))
            for device in data:
               
                #device = eval(device)
                new_host = str(device.get("device_name"))
                new_sev = str(device.get("severity"))
                new_age = str(device.get("age"))
                datasource = str(device.get("data_source"))
                try:                        
                    device_old_state = old_states.get(new_host)                     
                    old_state = device_old_state.get('state')
                    old_age = device_old_state.get('since')

                    if old_state!= new_sev:
                        #logging.info("State has changed for %s from %s to %s"%(new_host,old_state,new_sev))
                        old_states[new_host] = {'state':new_sev,'since':new_age}
                    else:
                        old_states[new_host] = {'state':old_state,'since':old_age}
                    #logging.info("State is same for %s as %s"%(new_host,old_state))
                except Exception:
                    logging.info("Unable to find host in old state for host %s " %new_host)
                    old_states[new_host] = {'state':new_sev,'since':new_age}
                    logging.info("Created new sate dict for %s as severity %s and since %s"%(new_host,new_sev,new_age))
                    continue              

        try:                
            
            if old_states != None and len(old_states) > 0:
                redis_hook_2.set("kpi_interference_prev_state_",str(device_type))
                logging.error("Succeessfully Updated Age")          
            else:
                logging.info("recieved fucking None") 

        except Exception:
            logging.error("Unable to actually insert the updated data into redis")

    except Exception:
        logging.info("Unable to get latest refer")
        traceback.print_exc()
  



initiate_dag = PythonOperator(
                task_id = "Initiate",
                provide_context=False,
                python_callable=init_kpi,
                dag=interference_dag,
                queue = Q_PUBLIC
                )


for machine in machines:
            for site in machine.get('sites'):
                site_name = site.get('name')
                all_sites.append(site_name)
                #site = eval(site)
                #print site.get('name')

for technology in technologies:
    CHILD_DAG_NAME = "%s"%(technology)
    #logging.info("For Tech %s"%(technology))
    ul_subdag_task=SubDagOperator(subdag=process_interference_dl_kpi(PARENT_DAG_NAME, CHILD_DAG_NAME, datetime(2017, 2, 24),interference_dag.schedule_interval,Q_PUBLIC,
        devices.get(technology).keys(),
        devices.get(technologies.get(technology)).keys(),
        devices.get(technology),
        devices.get(technologies.get(technology)),
        technology,
        technologies.get(technology),
        all_sites),
    task_id=CHILD_DAG_NAME,
    dag=interference_dag,
    queue=Q_PUBLIC
    )
    update_age_bs_task = PythonOperator(
                task_id = "update_bs_age_%s"%(technology),
                provide_context=True,
                python_callable=update_age,
                params={'type':'bs','device_type':technology},
                dag=interference_dag,
                queue = Q_PUBLIC
                )
    update_age_ss_task = PythonOperator(
                task_id = "update_ss_age_%s"%technologies.get(technology),
                provide_context=True,
                python_callable=update_age,
                params={'type':'ss','device_type':technologies.get(technology)},
                dag=interference_dag,
                queue = Q_PUBLIC
                )   

    initiate_dag >> ul_subdag_task 
    ul_subdag_task >> update_age_bs_task
    ul_subdag_task >> update_age_ss_task
    # aggregate_utilization_data = DummyOperator(
    #         task_id = "aggregate_interference_kpi_of_%s"%technology,
    #         #provide_context=True,
    #         #python_callable=aggregate_utilization_data,
    #         #params={"machine_name":each_machine_name},
    #         dag=interference_dag
    #         )   
