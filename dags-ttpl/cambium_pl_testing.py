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
    # 'sla' : timedelta(minutes=2)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "CAM_PL_TESTING"
Q_PUBLIC = "poller_queue"

cam_pl_testing_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='0 * * * *',)

system_config=eval(Variable.get('system_config'))
databases=eval(Variable.get('databases'))
services = eval(Variable.get('ml_services'))
cam_services = eval(Variable.get('cambium_services'))
#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################



for machine in system_config:

    machine_name=machine.get('Name')
    if machine_name == 'ospf2':

    	obj = ml_tf_implementer(network_key='nw_agg_nocout_%s' % machine_name 
    		    	    ,service_key ='sv_agg_nocout_%s' % machine_name
    		    	    ,services=services,cam_services=cam_services)
    	print "#####"
        pl_cam_test = PythonOperator(
             task_id="cam_pl_test_%s" % machine_name,
             provide_context=True,
             python_callable=obj.cambium_test_data,
             dag=cam_pl_testing_dag,
             params={'dag_obj': cam_pl_testing_dag},
             queue=Q_PUBLIC)

    
