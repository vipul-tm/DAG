from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import ExternalTaskSensor
from airflow.operators import MemcToMySqlOperator
from celery.signals import task_prerun, task_postrun

from pl_prediction import ml_tf_implementer 
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
    'sla': timedelta(minutes=2)
    # 'sla' : timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "PL_PREDICTION"
CHILD_DAG_NAME_NETWORK = "NETWORK"
CHILD_DAG_NAME_SERVICE = "SERVICE"
CHILD_DAG_NAME_EVENTS = "EVENTS" 
CHILD_DAG_NAME_TOPOLOGY="TOPOLOGY"
CHILD_DAG_NAME_DEVICE_DOWN="DEVICE_DOWN"
Q_PUBLIC = "poller_queue"
Q_PRIVATE = "formatting_queue"
Q_OSPF = "poller_queue"
Q_PING = "poller_queue"


CHILD_DAG_NAME_FORMAT = "FORMAT"
ml_prediction_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *',)

system_config=eval(Variable.get('system_config'))
databases=eval(Variable.get('databases'))
services = eval(Variable.get('ml_services'))
cam_services = eval(Variable.get('cambium_services'))

#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################



for machine in system_config:
    machine_name=machine.get('Name')
    if machine_name == 'ospf2' or machine_name == 'ospf1':
    	network_sensor = ExternalTaskSensor(
    	external_dag_id="ETL.FORMAT",
    	external_task_id="aggregate_%s_nw_data"%machine_name,
    	task_id="sense_%s_nw_aggregation"%machine_name,
    	poke_interval=2,
    	trigger_rule = 'all_done',
    	sla=timedelta(minutes=60),
    	dag=ml_prediction_dag,
    	queue=Q_PUBLIC,
    	)
    	service_sensor = ExternalTaskSensor(
    	external_dag_id="ETL.FORMAT",
    	external_task_id="aggregate_%s_sv_data"%machine_name,
    	task_id="sense_%s_sv_aggregation"%machine_name,
    	poke_interval =2,
    	trigger_rule = 'all_done',
    	#sla=timedelta(minutes=1),
    	dag=ml_prediction_dag,
    	queue=Q_PUBLIC,
    	)
    	obj = ml_tf_implementer(network_key='nw_agg_nocout_%s' % machine_name 
    		    	    ,service_key ='sv_agg_nocout_%s' % machine_name
    		    	    ,services=services,cam_services=cam_services)

    	pl_prediction_data_extraction = PythonOperator(
    	task_id="pl_prediction_data_extraction_%s" % machine_name,
    	provide_context=False,
    	python_callable=obj.gather,
    	dag=ml_prediction_dag,
    	queue=Q_PUBLIC)

    	pl_prediction_data_format_and_store = PythonOperator(
    	task_id="pl_prediction_data_format_and_store_%s" % machine_name,
    	provide_context=False,
    	python_callable=obj.format_data,
    	dag=ml_prediction_dag,
    	queue=Q_PUBLIC)
    	
    	network_sensor >> pl_prediction_data_extraction
    	service_sensor >> pl_prediction_data_extraction

    	pl_prediction_data_extraction >> pl_prediction_data_format_and_store
    	
