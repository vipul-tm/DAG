from airflow import DAG
from airflow.operators import ApiExtractor
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


PARENT_DAG_NAME = "TestModulesAPI"
main_etl_dag2=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *',)


te = ApiExtractor(
	task_id="test_modules",    
    #python_callable = upload_service_data_mysql,
    dag=main_etl_dag2,
    url = "http://date.jsontest.com/",
    api_params = {},
    request_type = "get",
    headers = {},
    redis_conn_id="redis_hook_4",
    identifier="API_TEST"
    )

