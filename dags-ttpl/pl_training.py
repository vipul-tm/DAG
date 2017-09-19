from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
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
PARENT_DAG_NAME = "PL_TRAINING"

Q_PUBLIC = "poller_queue"
Q_PRIVATE = "formatting_queue"
Q_OSPF = "poller_queue"
Q_PING = "poller_queue"



pl_training_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='0 * * * *',)

system_config=eval(Variable.get('system_config'))
databases=eval(Variable.get('databases'))
services = eval(Variable.get('ml_services'))
file_path = Variable.get('ml_features_data_path')
#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################


ml_data_task = BashOperator(
    task_id='pl_training',
    bash_command='python %s'%(file_path),
    dag=pl_training_dag,
    queue=Q_PUBLIC
    )
