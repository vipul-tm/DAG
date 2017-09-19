from airflow import DAG
#from airflow.operators import PythonOperator
#from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from subdags.utilization_kpi_by_technology import process_utilization_kpi
#from etl_tasks_functions import get_required_static_data
#from etl_tasks_functions import init_etl
#from etl_tasks_functions import debug_etl
#from etl_tasks_functions import send_data_to_kafka
#from airflow.operators import ExternalTaskSensor
#from airflow.operators import MemcToMySqlOperator
#from celery.signals import task_prerun, task_postrun
#from airflow.operators.kafka_extractor_operator import KafkaExtractorOperator

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

PARENT_DAG_NAME = "UTILIZATION_KPI"
Q_PUBLIC = "poller_queue"

utilization_kpi_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='*/5 * * * *',)

technologies = eval(Variable.get('utilization_kpi_technologies'))
attributes = eval(Variable.get('utilization_kpi_attributes'))
devices = eval(Variable.get('hostmk.dict.site_mapping'))

try:
    	for technology in technologies:

    		devices_by_technology = devices.get(technology)
    		attributes_by_technology = attributes.get(technology)

    		if devices_by_technology and attributes_by_technology:
            		utlization_kpi_by_technology = SubDagOperator( 
            			subdag = process_utilization_kpi(
    	    				PARENT_DAG_NAME, 
    	    				"Utilization_KPI_of_%s_devices"%(technology), 
    	    				datetime(2017, 9, 6), 
    	    				utilization_kpi_dag.schedule_interval, 
    	    				Q_PUBLIC,
    	    				technology,
    	    				devices_by_technology, 
    	    				attributes_by_technology,
    	    			),
    	    			task_id = "Utilization_KPI_of_%s_devices"%(technology),
    	    			dag = utilization_kpi_dag,
    	    			queue = Q_PUBLIC
    	    		)

except Exception as e:
    print "Exception in UTILIZATION KPI is: %s"%e


