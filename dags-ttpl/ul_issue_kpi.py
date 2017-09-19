from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from subdags.ul_issue_kpi_subdag import process_ul_issue_kpi
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

PARENT_DAG_NAME = "UL_ISSUE_KPI"
ul_issue_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *')

technologies = eval(Variable.get('ul_issue_kpi_technologies'))
machines = eval(Variable.get("system_config"))
devices = eval(Variable.get('hostmk.dict.site_mapping'))
all_sites=[]
for machine in machines:
            for site in machine.get('sites'):
                site_name = site.get('name')
                all_sites.append(site_name)
                #site = eval(site)
                #print site.get('name')

for technology in technologies:
    CHILD_DAG_NAME = "%s"%(technology)
    #logging.info("For Tech %s"%(technology))
    ul_subdag_task=SubDagOperator(subdag=process_ul_issue_kpi(PARENT_DAG_NAME, CHILD_DAG_NAME, datetime(2017, 2, 24),ul_issue_dag.schedule_interval,Q_PUBLIC,
        devices.get(technology).keys(),
        devices.get(technologies.get(technology)).keys(),
        devices.get(technology),
        devices.get(technologies.get(technology)),
        technology,
        technologies.get(technology),
        all_sites),
    task_id=CHILD_DAG_NAME,
    dag=ul_issue_dag,
    queue=Q_PUBLIC
    )
    # aggregate_utilization_data = DummyOperator(
    #         task_id = "aggregate_ul_issue_kpi_of_%s"%technology,
    #         #provide_context=True,
    #         #python_callable=aggregate_utilization_data,
    #         #params={"machine_name":each_machine_name},
    #         dag=ul_issue_dag
    #         )   
