from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from subdags.network import network_etl
from subdags.service import service_etl
from subdags.format import format_etl
from subdags.device_down_etl import device_down_etl
from subdags.topology import topology_etl
from subdags.events import calculate_events

from etl_tasks_functions import init_etl
from etl_tasks_functions import debug_etl

from airflow.operators import ExternalTaskSensor
from airflow.operators import MemcToMySqlOperator
from celery.signals import task_prerun, task_postrun


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
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "ETL"
CHILD_DAG_NAME_NETWORK = "NETWORK"
CHILD_DAG_NAME_SERVICE = "SERVICE"
CHILD_DAG_NAME_EVENTS = "EVENTS" 
CHILD_DAG_NAME_TOPOLOGY="TOPOLOGY"
CHILD_DAG_NAME_DEVICE_DOWN="DEVICE_DOWN"
Q_PUBLIC = "poller_queue"
Q_PRIVATE = "formatting_queue"
Q_OSPF = "poller_queue"
Q_PING = "poller_queue"

NW_DB_COLUMNS = "machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,warning_threshold,critical_threshold,device_name,sys_timestamp,refer,ip_address,data_source,check_timestamp,severity"
SV_DB_COLUMNS = "machine_name,site_name,critical_threshold,device_name,ip_address,data_source,current_value,check_timestamp,severity,service_name,age,sys_timestamp,warning_threshold,refer,avg_value,max_value,min_value"

CHILD_DAG_NAME_FORMAT = "FORMAT"
main_etl_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *',)
system_config=eval(Variable.get('system_config'))
databases=eval(Variable.get('databases'))
debug_mode = eval(Variable.get("debug_mode"))
ml_mode = eval(Variable.get("ml_mode"))
services = eval(Variable.get('ml_services'))
#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################


network_etl = SubDagOperator(
    subdag=network_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_NETWORK, datetime(2017, 2, 24),main_etl_dag.schedule_interval,Q_PUBLIC),
    task_id=CHILD_DAG_NAME_NETWORK,
    dag=main_etl_dag,
    queue=Q_PUBLIC
    )
service_etl = SubDagOperator(
    subdag=service_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_SERVICE, datetime(2017, 2, 24),main_etl_dag.schedule_interval,Q_PUBLIC),
    task_id=CHILD_DAG_NAME_SERVICE,
    dag=main_etl_dag,
    queue=Q_PUBLIC

    )
format_etl = SubDagOperator(
    subdag=format_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_FORMAT, datetime(2017, 2, 24),main_etl_dag.schedule_interval,Q_PUBLIC),
    task_id=CHILD_DAG_NAME_FORMAT,
    dag=main_etl_dag,
    queue=Q_PUBLIC
    )
#topology_etl = SubDagOperator(
#    subdag=topology_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_TOPOLOGY, datetime(2017, 2, 24),main_etl_dag.schedule_interval),
#    task_id=CHILD_DAG_NAME_TOPOLOGY,
#    dag=main_etl_dag,
#    )

#Created seperate subdaga as otherwise topology would be dependent onto servioce which might take huge time 
device_down_etl = SubDagOperator(
    subdag=device_down_etl(PARENT_DAG_NAME, CHILD_DAG_NAME_DEVICE_DOWN, datetime(2017, 2, 24),main_etl_dag.schedule_interval,Q_PUBLIC),
    task_id=CHILD_DAG_NAME_DEVICE_DOWN,
    dag=main_etl_dag,
    queue=Q_PUBLIC
    )
# events = SubDagOperator(
#     subdag=calculate_events(PARENT_DAG_NAME, CHILD_DAG_NAME_EVENTS, datetime(2017, 2, 24),main_etl_dag.schedule_interval),
#     task_id=CHILD_DAG_NAME_EVENTS,
#     dag=main_etl_dag,
#     )




initiate_etl = PythonOperator(
    task_id="Initiate",
    provide_context=False,
    python_callable=init_etl,
    #params={"FORMAT_QUEUE":FORMAT_QUEUE},
    dag=main_etl_dag,
    queue=Q_PUBLIC)
if ml_mode:
    kafka_extractor = KafkaExtractorOperator(
        task_id="push",    
        #python_callable = upload_service_data_mysql,
        dag=main_etl_dag,
        redis_conn='redis_hook_4',
        kafka_con_id='kafka_default',
        topic='ml_queue',
        identifier_output='redID',
        output_identifier_index='redIDInd',
        index_key='timestamp',
        #skeleton_dict={'b':'t'},
        indexed=False,
        queue=Q_PUBLIC
        )
    kafka_producer = PythonOperator(
        task_id="push_data_to_kafka",
        provide_context=False,
        python_callable=send_data_to_kafka,
        #params={"redis_hook_2":redis_hook_2},
        dag=main_etl_dag,
        queue=Q_PUBLIC)

if debug_mode:
    debug_data= PythonOperator(
    task_id="DEBUG",
    provide_context=False,
    python_callable=debug_etl,
    #params={"FORMAT_QUEUE":FORMAT_QUEUE},
    dag=main_etl_dag,
    queue=Q_PUBLIC)

for db in databases:
    machine = db.split("_")[1]
    database_name = "nocout_%s"%machine
################################################################QUERIS WITH DB NAME MUST BE IN FOR LOOP,Coz They are loopers#####################################################################################
    
    query_nw = "INSERT INTO %s.performance_performancenetwork "%database_name
    query_nw +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s)"  

    #query_nw_update = "REPLACE INTO %s.performance_networkstatus "%database_name
    #query_nw_update +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s)"  

    query_nw_update = "INSERT INTO %s.performance_networkstatus "%database_name
    query_nw_update +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s) ON DUPLICATE KEY UPDATE machine_name = VALUES(machine_name),current_value = VALUES(current_value),age=VALUES(age),site_name=VALUES(site_name),critical_threshold=VALUES(critical_threshold),severity=VALUES(severity),sys_timestamp=VALUES(sys_timestamp),ip_address=VALUES(ip_address),warning_threshold=VALUES(warning_threshold),check_timestamp=VALUES(check_timestamp),refer=VALUES(refer)"  

    query_sv = "INSERT INTO %s.performance_performanceservice "%database_name
    query_sv +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s)"  

    #query_sv_update = "REPLACE INTO %s.performance_servicestatus "%database_name
    #query_sv_update +="(machine_name,current_value,service_name,age,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(age)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s)"  

    query_sv_update = "INSERT INTO %s.performance_servicestatus "%database_name
    query_sv_update +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values (%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s,%(refer)s) ON DUPLICATE KEY UPDATE machine_name = VALUES(machine_name),current_value = VALUES(current_value),avg_value = VALUES(avg_value),max_value = VALUES(max_value),min_value = VALUES(min_value),age = VALUES(age),site_name=VALUES(site_name),critical_threshold=VALUES(critical_threshold),severity=VALUES(severity),sys_timestamp=VALUES(sys_timestamp),ip_address=VALUES(ip_address),warning_threshold=VALUES(warning_threshold),check_timestamp=VALUES(check_timestamp),refer=VALUES(refer)"  


########################################################################################################################################################


    insert_sv_data = MemcToMySqlOperator(
    task_id="upload_service_data_mysql_%s"%db,    
    #python_callable = upload_service_data_mysql,
    dag=main_etl_dag,
    mysql_table="performance_performanceservice",
    memc_key="sv_agg_"+str(db),
    exec_type = 0,
    sql=query_sv,
    db_coloumns=SV_DB_COLUMNS,
    trigger_rule = 'all_done',
    queue=Q_PUBLIC,
    )
    insert_nw_data = MemcToMySqlOperator(
    task_id="upload_network_data_mysql_%s"%db,
    #python_callable = "upload_network_data_mysql",
    dag=main_etl_dag,
    mysql_table="performance_performancenetwork",
    memc_key="nw_agg_"+str(db),
    exec_type = 0,
    sql=query_nw,
    db_coloumns = NW_DB_COLUMNS,
    trigger_rule = 'all_done',
    queue=Q_PUBLIC,
    )
    update_nw_data = MemcToMySqlOperator(
    task_id="update_network_data_mysql_%s"%db,    
    #python_callable = "upload_network_data_mysql",
    dag=main_etl_dag,
    mysql_table="performance_networkstatus",
    memc_key="nw_agg_"+str(db),
    exec_type = 0,
    sql=query_nw_update,
    update = True,
    db_coloumns = NW_DB_COLUMNS,
    trigger_rule = 'all_done',
    queue=Q_PUBLIC,
    )

    update_sv_data = MemcToMySqlOperator(
    task_id="update_service_data_mysql_%s"%db,    
    #python_callable = "upload_network_data_mysql",
    dag=main_etl_dag,
    mysql_table="performance_servicestatus",
    memc_key="sv_agg_"+str(db),
    exec_type = 0,
    sql=query_sv_update,
    update = True,
    db_coloumns = SV_DB_COLUMNS,
    trigger_rule = 'all_done',
    queue=Q_PUBLIC,
   )
    network_sensor = ExternalTaskSensor(
    external_dag_id="ETL.FORMAT",
    external_task_id="aggregate_%s_nw_data"%machine,
    task_id="sense_%s_nw_aggregation"%machine,
    poke_interval=2,
    trigger_rule = 'all_done',
    #sla=timedelta(minutes=1),
    dag=main_etl_dag,
    queue=Q_PUBLIC,
    )
    service_sensor = ExternalTaskSensor(
    external_dag_id="ETL.FORMAT",
    external_task_id="aggregate_%s_sv_data"%machine,
    task_id="sense_%s_sv_aggregation"%machine,
    poke_interval =2,
    trigger_rule = 'all_done',
    #sla=timedelta(minutes=1),
    dag=main_etl_dag,
    queue=Q_PUBLIC,
    )
    



    service_sensor >> insert_sv_data
    network_sensor >> insert_nw_data
    network_sensor >> update_nw_data
    service_sensor >> update_sv_data
    network_etl >> network_sensor
    service_etl >> service_sensor

    if ml_mode:
        insert_sv_data >> kafka_producer
        insert_nw_data >> kafka_producer
        #update_sv_data >> kafka_producer
        #update_nw_data >> kafka_producer


   
    if debug_mode:
        insert_sv_data >> debug_data
        insert_nw_data >> debug_data
        #update_nw_data >> debug_data
        #update_sv_data >> debug_data



initiate_etl >> device_down_etl
device_down_etl >> service_etl
#device_down_etl >> topology_etl
initiate_etl >> network_etl

initiate_etl >> format_etl
if ml_mode:
    ml = DummyOperator(task_id='Process_ml', dag=main_etl_dag, queue=Q_PUBLIC)
    kafka_producer >> ml >> kafka_extractor




#format_etl >> events


############################################DEBUGGERS###################################################################################################
# d = {}

# @task_prerun.connect
# def task_prerun_handler(signal, sender, task_id, task, args, kwargs):
#     d[task_id] = time()


# @task_postrun.connect
# def task_postrun_handler(signal, sender, task_id, task, args, kwargs, retval, state):
#     try:
#         cost = time() - d.pop(task_id)
#     except KeyError:
#         cost = -1
#     print task.__name__ + ' ' + str(cost)
