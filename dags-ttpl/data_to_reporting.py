from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta    
from airflow.models import Variable
from airflow.hooks import RedisHook
from airflow.hooks import MemcacheHook
import logging
import traceback
from etl_tasks_functions import get_from_socket
from sync import get_host_ip_mapping
from airflow.hooks import MySqlHook
import sys




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
redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
memc = MemcacheHook(memc_cnx_id = 'memc_cnx')
PARENT_DAG_NAME = "REPORTING_DATA_DUMP"

Q_PUBLIC = "calculation_q"
Q_PRIVATE = "private_q"
Q_OSPF = "poller_queue"



main_reporting_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='9 16,11 * * *',)
config = eval(Variable.get('system_config_reporting'))
debug_mode = eval(Variable.get("debug_mode"))
services_required = ['cam450i_beacon'  ,'cam450i_ss_device_uptime'  ,'cam450i_ss_dl_modulation'  ,'cam450i_ss_dl_rssi_1'  ,'cam450i_ss_dl_snr_1'  ,'cam450i_ss_frequency'  ,'cam450i_ss_reg_count'  ,'cam450i_ss_session_uptime_system'  ,'cam450i_ss_ul_modulation'  ,'cam450i_ss_ul_rssi_1'  ,'cam450i_ss_ul_snr1'  ,'cam450m_ss_beacon'  ,'cam450m_ss_dl_modulation'  ,'cam450m_ss_dl_rssi_rf_port_1'  ,'cam450m_ss_dl_snr_rf_port_1'  ,'cam450m_ss_frequency'  ,'cam450m_ss_reg_count'  ,'cam450m_ss_session_uptime_system'  ,'cam450m_ss_ul_modulation'  ,'cam450m_ss_ul_rssi_rf_port_1'  ,'cam450m_ss_ul_snr_rf_port_1'  ,'cam450m_ss_uptime'  ,'cambium_dl_rssi'  ,'cambium_reg_count'  ,'cambium_session_uptime_system'  ,'cambium_ss_beacon'  ,'cambium_ss_frequency'  ,'cambium_ul_rssi'  ,'cambium_uptime'  ,'rad5k_dl_rssi'  ,'rad5k_ss_device_uptime'  ,'rad5k_ss_dl_bbe'  ,'rad5k_ss_dl_es'  ,'rad5k_ss_dl_estmd_throughput'  ,'rad5k_ss_dl_modulation'  ,'rad5k_ss_dl_ses'  ,'rad5k_ss_dl_uas'  ,'rad5k_ss_dl_utilization '  ,'rad5k_ss_frequency'  ,'rad5k_ss_mac'  ,'rad5k_ss_sector_id'  ,'rad5k_ss_transmit_power '  ,'rad5k_ss_ul_estmd_throughput'  ,'rad5k_ss_ul_modulation'  ,'rad5k_ss_ul_utilization '  ,'rad5k_ss2_dl_rssi'  ,'rad5k_ss2_ul_rssi'  ,'rad5k_ul_rssi'  ,'rad5kjet_ss_bbe'  ,'rad5kjet_ss_dl_estmd_throughput'  ,'rad5kjet_ss_dl_modulation'  ,'rad5kjet_ss_dl_rssi_1'  ,'rad5kjet_ss_dl_utilization'  ,'rad5kjet_ss_es'  ,'rad5kjet_ss_frequency'  ,'rad5kjet_ss_sector_id'  ,'rad5kjet_ss_ses'  ,'rad5kjet_ss_uas'  ,'rad5kjet_ss_ul_estmd_throughput'  ,'rad5kjet_ss_ul_modulation'  ,'rad5kjet_ss_ul_rssi_1'  ,'rad5kjet_ss_ul_utilization'  ,'rad5kjet_ss_uptime'  ,'radwin_crc_errors'  ,'radwin_dl_utilization'  ,'radwin_rssi'  ,'radwin_service_throughput'  ,'radwin_uas'  ,'radwin_ul_utilization'  ,'radwin_uptime'  ,'wimax_dl_cinr'  ,'wimax_dl_intrf'  ,'wimax_dl_rssi'  ,'wimax_modulation_dl_fec'  ,'wimax_modulation_ul_fec'  ,'wimax_ss_dl_utilization'  ,'wimax_ss_frequency'  ,'wimax_ss_session_uptime'  ,'wimax_ss_ul_utilization'  ,'wimax_ss_uptime'  ,'wimax_ul_cinr'  ,'wimax_ul_intrf'  ,'wimax_ul_rssi']
status_service_required = ['cam450i_ss_ap_ip_status','cam450m_ss_ap_ip_status','rad5k_ss_crc_error_status','rad5kjet_ss_crc_error_status','radwin_port_mode_status']
Q_SERVICE = """INSERT INTO performance_performanceservice_rfhealthstatus 
(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) 
values 
(%(machine_name)s,%(current_value)s,%(service)s,%(current_value)s,%(current_value)s,%(age)s,%(current_value)s,%(site)s,%(ds)s,%(cric)s,%(host)s,%(severity)s,%(local_timestamp)s,%(ip_address)s,%(war)s,%(check_time)s,%(refer)s);"""

Q_NETWORK = """INSERT INTO performance_performancenetwork_rfhealthstatus 
(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) 
values 
(%(machine_name)s,%(current_value)s,%(service)s,%(current_value)s,%(current_value)s,%(age)s,%(current_value)s,%(site)s,%(ds)s,%(cric)s,%(host)s,%(severity)s,%(local_timestamp)s,%(ip_address)s,%(war)s,%(check_time)s,%(refer)s);"""


Q_STATUS = """INSERT INTO performance_performancestatus_rfhealthstatus 
(machine_name,current_value,service_name,avg_value,max_value,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp ) 
values 
(%(machine_name)s,%(current_value)s,%(service_name)s,%(avg_value)s,%(max_value)s,%(min_value)s,%(site_name)s,%(data_source)s,%(critical_threshold)s,%(device_name)s,%(severity)s,%(sys_timestamp)s,%(ip_address)s,%(warning_threshold)s,%(check_timestamp)s);"""


#############SERVICE##################
#{'site': 'ospf1_slave_1', 'host': '100', 'meta': {'cur': u'33', 'war': u'40', 'cric': u'45'}, 'ip_address': u'10.191.34.3', 'ds': u'acb_temp',
# 'check_time': 1517986465, 'severity': 'ok', 'service': 'wimax_bs_temperature_acb', 'data': [{'value': u'33', 'time': 1517986465}], 
#'age': 1515116399, 'local_timestamp': 1517986525, 'refer': ''}

#############NETWORK###############
#"{'service': 'ping', 'severity': 'up', 'check_time': 1517986392, 'ip_address': '10.191.34.3',
#'age': 1517952604, 'local_timestamp': 1517986512, 'site': 'ospf1_slave_1', 'host': '100', 'meta': {'cur': u'0', 'war': u'10', 'cric': u'20'},
#'data': [{'value': u'0', 'time': 1517986392}], 'ds': u'pl', 'refer': ''}

##########STATUS#####################
#{'data_source': u'crc_errors', 'site_name': 'ospf2_slave_1', 'critical_threshold': u'', 'severity': 'ok', 
#'avg_value': 0, 'max_value': 0, 'min_value': 0, 'check_timestamp': 1518000504, 'device_name': '358',
# 'sys_timestamp': 1518000504, 'service_name': u'rad5k_bs_crc_error_status', 'current_value': u'0', 
# 'ip_address': u'10.191.121.4', 'warning_threshold': u''}
#######################################DAG Config Ends ####################################################################################################################
###########################################################################################################################################################################
#######################################TASKS###############################################################################################################################


def filter_service_data(services_data,network_data):
    service_data_filtered = []
    status_service_data_filtered = []
    network_data_filtered = []
    for dev in services_data:
        dev = eval(dev)
        if dev.get('service') in services_required:
            dev['current_value'] = dev.get('meta').get('cur')
            dev['war'] = dev.get('meta').get('war')
            dev['cric'] = dev.get('meta').get('cric')
            dev['machine_name'] = dev.get('site').split("_")[0]
            dev.pop('meta')
            dev.pop('data')
            service_data_filtered.append(dev.copy())

        elif dev.get('service_name') in status_service_required:
            dev['machine_name'] = dev.get('site_name').split("_")[0]
            status_service_data_filtered.append(dev.copy())

    for dev in network_data:
        dev=eval(dev)
        dev['current_value'] = dev.get('meta').get('cur')
        dev['war'] = dev.get('meta').get('war')
        dev['cric'] = dev.get('meta').get('cric')
        dev['machine_name'] = dev.get('site').split("_")[0] 
        dev.pop('meta')
        dev.pop('data')
        network_data_filtered.append(dev.copy())


    return [service_data_filtered,status_service_data_filtered,network_data_filtered]


 #Func to get devices which are down so as not to process those
def get_all_devices_data(**kwargs):
    service_list = []
    network_list = []
    for poller in config:
        poller_name = poller.get('Name')
        if poller_name not in ['pub','vrfprv']:
            poller_number = poller_name.split('f')[1]
        else:
            if poller_name == "pub":
                poller_number = 6
            elif poller_name == "vrfprv":
                poller_number = 7
        logging.info("Poller NO %s"%(poller_number))
        redis_connection_poller = RedisHook(redis_conn_id="redis_prev_state_%s"%(poller_number))
        try:
            #print type(redis_connection_poller.rget('service_data_reporting')),redis_connection_poller.rget('network_data_reporting')[0],redis_connection_poller.rget('service_data_reporting')[0]
            service_list.extend(redis_connection_poller.rget('service_data_reporting'))
            network_list.extend(redis_connection_poller.rget('network_data_reporting'))
        except Exception:
            logging.warning("Unable to get the data from %s redis "%(poller_name))

    logging.info("Network Len : %s Service Len %s"%(len(service_list),len(network_list)))
    #here we have 3 list which are to be inserted in 3 tables lets do it serially only or this may increase load
    
    filtered_data = filter_service_data(service_list,network_list) #added currnt_value cric and war from meta to main dic

    service_list_filtered = filtered_data[0]
    status_service_list_filtered = filtered_data[1]
    network_list_filtered = filtered_data[2]

    #logging.info("S:%s \n N: %s \n SS: %s "%(len(service_list_filtered),len(network_list_filtered),len(status_service_list_filtered)))
    print status_service_list_filtered
    try:
        hook = MySqlHook(mysql_conn_id='reporting_server_mysql')
        conn = hook.get_conn()
        cursor = conn.cursor()
        if len(service_list_filtered) > 0:
            cursor.execute("truncate performance_performanceservice_rfhealthstatus;")
            cursor.executemany(Q_SERVICE, service_list_filtered)
        if len(network_list_filtered) > 0:
            cursor.execute("truncate performance_performancenetwork_rfhealthstatus;")
            cursor.executemany(Q_NETWORK, network_list_filtered)
        if len(status_service_list_filtered) > 0:
            cursor.execute("truncate performance_performancestatus_rfhealthstatus;")
            cursor.executemany(Q_STATUS, status_service_list_filtered)
        conn.commit()
        cursor.close()
        conn.close()
    except Exception:
        logging.error("Some Exception...")
        traceback.print_exc()
        cursor.close()
        conn.close()

device_down_task = PythonOperator(
task_id="dump_data_to_reporting_server",
provide_context=True,
python_callable=get_all_devices_data,
dag=main_reporting_dag,
queue = Q_OSPF
)







