from airflow import DAG
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.operators.subdag_operator import SubDagOperator
from subdags.ul_issue_kpi_subdag import process_ul_issue_kpi
from collections import defaultdict
from airflow.hooks import RedisHook

from airflow.operators import ExternalTaskSensor
import csv
import MySQLdb
import MySQLdb.cursors
import time
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

PARENT_DAG_NAME = "TEST_KPI"

test_ul_issue_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='3-59/5 * * * *')
redis_hook_6 = RedisHook(redis_conn_id="redis_hook_6")
redis_hook_2 = RedisHook(redis_conn_id="redis_hook_2")
redis_hook_7 = RedisHook(redis_conn_id="redis_hook_7")
def get_omd_data(sql,DB):
    db = MySQLdb.connect(host="121.244.255.108",
                     user="nocout_admin",        
                     passwd="nocout_root_master_UA@123",  
                     db=DB, 
             port=3200
             )       

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    #cur = db.cursor()
    cursor_dict = db.cursor(MySQLdb.cursors.DictCursor) 


    # Use all the SQL you like
   
    #cur.execute("select * from inventory_basestations")
    #cursor_dict.execute("truncate inventory_coverageanalyticsprocessedinfo;")

    cursor_dict.execute(sql)
    # print all the first cell of all the rows
    all_data = []
    for row in cursor_dict.fetchall():
       all_data.append(row)

   

    db.close()
    return all_data



def test_ul_issue_kpi(**kwargs):

    omd_sv_data = []
    ulissue_keys = redis_hook_6.get_keys("aggregated_*")
    skipping_count = 0
    service = {}

    attributes = []
    combined_data = []

    for key in ulissue_keys:
        data = eval(redis_hook_6.get(key))
        if len(data) > 0:
            for device_dict in data:
                              
                service["%s_%s_%s"%(device_dict.get("device_name"),device_dict.get("service_name"),device_dict.get("data_source"))] = device_dict
        else:
            logging.info("No Data")

    #['nocout_ospf1','nocout_ospf2','nocout_ospf3','nocout_ospf4','nocout_ospf5','nocout_ospf6','nocout_pub','nocout_vrfprv']

    for DB in ['nocout_ospf1']:
        d = get_omd_data("select sys_timestamp from %s.performance_utilizationstatus order by sys_timestamp desc limit 1;"%(DB),DB)
        max_time = d[0].get('sys_timestamp')#and (service_name = 'wimax_ss_ul_issue_kpi' or service_name = 'wimax_bs_ul_issue_kpi')
        omd_kpi_data_per_poller = get_omd_data("SELECT * FROM %s.performance_utilizationstatus  where sys_timestamp between %s and %s and (data_source = 'pmp1_ul_issue' or data_source = 'pmp2_ul_issue' or data_source = 'ul_issue' or data_source = 'bs_ul_issue');"%(DB,max_time-300,max_time),DB)
        omd_sv_data.extend(omd_kpi_data_per_poller)

    all_data_sv = []
    header_sv = ["Hostname","ds","ip","AF cur","OMD cur","Decision","AF Severity","OMD Severity","Decision","AF Age","OMD Age","Decision","AF Time","OMD Time"]
    all_data_sv.append(header_sv)

    for omd_service in omd_sv_data:
        current_dev_key = "%s_%s_%s"%(str(omd_service.get('device_name')),str(omd_service.get('service_name')),omd_service.get("data_source"))
        if service.get(current_dev_key):
            #print "%s=%s and %s=%s"%(service.get(current_dev_key).get('device_name') , omd_service.get('device_name') , service.get(current_dev_key).get('service_name'), omd_service.get('service_name'))    
            if int(service.get(current_dev_key).get('device_name')) == int(omd_service.get('device_name')) and str(service.get(current_dev_key).get('service_name')).strip() == str(omd_service.get('service_name')).strip():
                #print "T %s %s %s %s"%((str(service.get('host'))) , (str(omd_service.get('device_name'))) , (str(service.get('service') )),(str(omd_service.get('service_name'))))
                excel_row = [service.get(current_dev_key).get('device_name'),service.get(current_dev_key).get('service_name')+str(service.get(current_dev_key).get('data_source')),service.get(current_dev_key).get('ip_address'),service.get(current_dev_key).get('current_value') \
                ,omd_service.get('current_value'),"True" if str(service.get(current_dev_key).get('current_value')) == str(omd_service.get('current_value')) else "False" ,\
                str(service.get(current_dev_key).get('severity')),str(omd_service.get('severity')),"True" if str(service.get(current_dev_key).get('severity')) == str(omd_service.get('severity')) else "False" ,\
                str(service.get(current_dev_key).get('age')),str(omd_service.get('age')),"True" if str(service.get(current_dev_key).get('age')) == str(omd_service.get('age')) else "False" ,
                str(service.get(current_dev_key).get('sys_timestamp')),str(omd_service.get('sys_timestamp'))
                ]
                all_data_sv.append(excel_row)
            else:
                #print "continue : %s == %s and %s == %s"%(type(str(service.get('host'))) , type(str(omd_service.get('device_name'))) , type(str(service.get('service') )),type (str(omd_service.get('service_name'))))
                print "**************************FISHY******************************"
                continue
        else:
            #print "Skipping %s"%current_dev_key
            #if "radwin" not in omd_service.get('service_name'):
            #   print omd_service.get('service_name'),omd_service.get('site_name')
            #else:
            #   radwin_count = radwin_count + 1
            print current_dev_key 
            skipping_count = skipping_count + 1
            
    logging.info("Total Skipped %s"%(skipping_count))
    logging.info("Total Devices From DB %s"%(len(omd_sv_data)))
    logging.info("Total Devices From Airflow %s"%(len(service)))
    logging.info("Total Processed Devices %s"%(len(omd_sv_data) - skipping_count ))
    summary=[len(omd_sv_data),len(service),skipping_count,len(omd_sv_data) - skipping_count]
    time_str=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open("/home/tmadmin/NITIN/VIPUL/logs/comparision-%s.csv"%(time_str),"wb") as out:
        wr = csv.writer(out)        
        for datalist in all_data_sv:
            wr.writerow(datalist)

    with open("/home/tmadmin/NITIN/VIPUL/summary.csv","ab") as out:
        wr2 = csv.writer(out)        
        wr2.writerow(summary)
    

def test_provis_kpi(**kwargs):
    provis_keys = redis_hook_7.get_keys("aggregated_*")
    omd_sv_data = []
    data_to_page_provis = []
    attributes = []
    combined_data = []
    service = {}
    skipping_count = 0
    for key in provis_keys:
        data = eval(redis_hook_7.get(key))
        if len(data) > 0:
            for device_dict in data:
                              
                service["%s_%s_%s"%(device_dict.get("device_name"),device_dict.get("service_name"),device_dict.get("data_source"))] = device_dict
        else:
            logging.info("No Data")

   #['nocout_ospf1','nocout_ospf2','nocout_ospf3','nocout_ospf4','nocout_ospf5','nocout_ospf6','nocout_pub','nocout_vrfprv']

    for DB in ['nocout_ospf1']:
        d = get_omd_data("select sys_timestamp from %s.performance_utilizationstatus order by sys_timestamp desc limit 1;"%(DB),DB)
        max_time = d[0].get('sys_timestamp')#and (service_name = 'wimax_ss_ul_issue_kpi' or service_name = 'wimax_bs_ul_issue_kpi')
        omd_kpi_data_per_poller = get_omd_data("SELECT * FROM %s.performance_utilizationstatus  where sys_timestamp between %s and %s and (service_name = 'cambium_ss_provis_kpi' or service_name = 'wimax_ss_provis_kpi' or service_name = 'radwin_ss_provis_kpi');"%(DB,max_time-300,max_time),DB)
        omd_sv_data.extend(omd_kpi_data_per_poller)

    all_data_sv = []
    #header_sv = ["Hostname","ds","ip","AF cur","OMD cur","Decision","AF Severity","OMD Severity","Decision","AF Age","OMD Age","Decision"]
    header_sv = []
    omd_header = omd_sv_data[0].keys()
    for k in  omd_header:
        header_sv.append("AF_%s"%(k))
        header_sv.append("OMD_%s"%(k))
        header_sv.append("Result")

        

    all_data_sv.append(header_sv)

    for omd_service in omd_sv_data:
        current_dev_key = "%s_%s_%s"%(str(omd_service.get('device_name')),str(omd_service.get('service_name')),omd_service.get("data_source"))
        if service.get(current_dev_key):
            #print "%s=%s and %s=%s"%(service.get(current_dev_key).get('device_name') , omd_service.get('device_name') , service.get(current_dev_key).get('service_name'), omd_service.get('service_name'))    
            if int(service.get(current_dev_key).get('device_name')) == int(omd_service.get('device_name')) and str(service.get(current_dev_key).get('service_name')).strip() == str(omd_service.get('service_name')).strip():
                #print "T %s %s %s %s"%((str(service.get('host'))) , (str(omd_service.get('device_name'))) , (str(service.get('service') )),(str(omd_service.get('service_name'))))
                device_row = []
                excel_row = []
                for key in omd_service.keys():
                    af_val = str(service.get(current_dev_key).get(key))
                    omd_val = str(omd_service.get(key))
                    excel_row.append(af_val)
                    excel_row.append(omd_val)
                    excel_row.append("True" if str(af_val) == str(omd_val) else "False")

                    

                # excel_row = [service.get(current_dev_key).get('device_name'),service.get(current_dev_key).get('service_name'),service.get(current_dev_key).get('ip_address'),service.get(current_dev_key).get('current_value') \
                # ,omd_service.get('current_value'),"True" if str(service.get(current_dev_key).get('current_value')) == str(omd_service.get('current_value')) else "False" ,\
                # str(service.get(current_dev_key).get('severity')),str(omd_service.get('severity')),"True" if str(service.get(current_dev_key).get('severity')) == str(omd_service.get('severity')) else "False" ,\
                # str(service.get(current_dev_key).get('age')),str(omd_service.get('age')),"True" if str(service.get(current_dev_key).get('age')) == str(omd_service.get('age')) else "False" ,
                # ]
                all_data_sv.append(excel_row)
            else:
                #print "continue : %s == %s and %s == %s"%(type(str(service.get('host'))) , type(str(omd_service.get('device_name'))) , type(str(service.get('service') )),type (str(omd_service.get('service_name'))))
                print "**************************FISHY******************************"
                continue
        else:
            #print "Skipping %s"%current_dev_key
            #if "radwin" not in omd_service.get('service_name'):
            #   print omd_service.get('service_name'),omd_service.get('site_name')
            #else:
            #   radwin_count = radwin_count + 1
            
            row = [str(omd_service.get('device_name')),str(omd_service.get('service_name')),str(omd_service.get('current_value')),str(omd_service.get('ip_address'))]
            all_data_sv.append(row)
            skipping_count = skipping_count + 1
            
    logging.info("Total Skipped %s"%(skipping_count))
    logging.info("Total Devices From DB %s"%(len(omd_sv_data)))
    logging.info("Total Devices From Airflow %s"%(len(service)))
    logging.info("Total Processed Devices %s"%(len(omd_sv_data) - skipping_count ))
    summary=[len(omd_sv_data),len(service),skipping_count,len(omd_sv_data) - skipping_count]
    time_str=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open("/home/tmadmin/NITIN/VIPUL/logs/provis/comparision-provis-%s.csv"%(time_str),"wb") as out:
        wr = csv.writer(out)        
        for datalist in all_data_sv:
            wr.writerow(datalist)

    with open("/home/tmadmin/NITIN/VIPUL/summary-provis.csv","ab") as out:
        wr2 = csv.writer(out)        
        wr2.writerow(summary)




test_ul_issue_kpi = PythonOperator(
            task_id = "test_ul_issue",
            provide_context=True,
            python_callable=test_ul_issue_kpi,
            dag=test_ul_issue_dag,
            queue = Q_PUBLIC
            )

test_provis_kpi = PythonOperator(
           task_id = "test_provision_kpi",
           provide_context=True,
           python_callable=test_provis_kpi,
           dag=test_ul_issue_dag,
           queue = Q_PUBLIC
           )


ULISSUE = ExternalTaskSensor(
    external_dag_id="UL_ISSUE_KPI.StarmaxIDU",
    external_task_id="aggregate_ul_issue_bs_ospf1",
    task_id="sense_ospf1_ul_issue",
    poke_interval=20,
    trigger_rule = 'all_done',
    #sla=timedelta(minutes=1),
    dag=test_ul_issue_dag,
    queue=Q_PUBLIC,
    )
provis = ExternalTaskSensor(
    external_dag_id="PROVISION_KPI.StarmaxSS",
    external_task_id="aggregate_provision_ss_ospf1",
    task_id="sense_ospf1_proviosion",
    poke_interval=20,
    trigger_rule = 'all_done',
    #sla=timedelta(minutes=1),
    dag=test_ul_issue_dag,
    queue=Q_PUBLIC,
    )
ULISSUE >> test_ul_issue_kpi
provis >> test_provis_kpi
