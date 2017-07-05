from airflow import DAG
from airflow.operators import TelradExtractor
from datetime import datetime, timedelta  
from airflow.models import Variable 
#from airflow.telrad_extractor_operator import TelradExtractor




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
    'sla' : timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}

PARENT_DAG_NAME = "TestModules"
main_etl_dag2=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *',)


te = TelradExtractor(
	task_id="test_modules",    
    #python_callable = upload_service_data_mysql,
    dag=main_etl_dag2,
    query = Variable.get("cpe_command_get_snapshot"),
    telrad_conn_id = "telrad_default",
    redis_conn_id = "redis_hook_4",
    wait_str = "admin@BreezeVIEW>"
    )

