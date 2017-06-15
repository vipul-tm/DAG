import os
import socket
from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.operators import PythonOperator
from airflow.operators import DummyOperator
from airflow.operators import BashOperator
from airflow.operators import BranchPythonOperator
from airflow.hooks import RedisHook
from datetime import datetime, timedelta	
from airflow.models import Variable
from airflow.operators import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
import itertools
import socket
import sys
import time
import re
import random
import logging
import traceback
import os
import json

#TODO: Commenting 

default_args = {
    'owner': 'wireless',
    'depends_on_past': False,
    'start_date': datetime(2017, 03, 30,13,00),
    'email': ['vipulsharma144@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'provide_context': True,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
     
}
#redis_hook = RedisHook(redis_conn_id="redis_4")
PARENT_DAG_NAME = "ServiceDAG"

main_service_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once')

def connect_and_extract():
        try:
	    site_name = 'ospf1_slave_1'
	    mrc_hosts = []
            st = datetime.now()
            current = int(time.time())
	    mrc_query = "GET services\nColumns: host_name service_state host_state host_address plugin_output\n"+ \
                        "Filter: service_description ~ wimax_pmp1_sector_id_invent\n"+\
                        "Filter: service_description ~ wimax_pmp2_sector_id_invent\nOr: 2\nOutputFormat: python\n"

            network_perf_query = "GET hosts\nColumns: host_name host_address host_state last_check host_last_state_change host_perf_data\nOutputFormat: python\n"
	    service_perf_query = "GET services\nColumns: host_name host_address service_description service_state "+\
                            "last_check service_last_state_change host_state service_perf_data\nFilter: service_description ~ _invent\n"+\
                            "Filter: service_description ~ _status\n"+\
                            "Filter: service_description ~ Check_MK\n"+\
                            "Filter: service_description ~ PING\n"+\
                            "Filter: service_description ~ .*_kpi\n"+\
                            "Filter: service_description ~ wimax_ss_port_params\n"+\
                            "Filter: service_description ~ wimax_bs_ss_params\n"+\
                            "Filter: service_description ~ wimax_aggregate_bs_params\n"+\
                            "Filter: service_description ~ wimax_bs_ss_vlantag\n"+\
                            "Filter: service_description ~ wimax_topology\n"+\
                            "Filter: service_description ~ cambium_topology_discover\n"+\
                            "Filter: service_description ~ mrotek_port_values\n"+\
                            "Filter: service_description ~ rici_port_values\n"+\
                            "Filter: service_description ~ rad5k_topology_discover\n"+\
                            "Or: 14\nNegate:\nOutputFormat: python\n"
	    #device_down_query = "GET services\nColumns: host_name\nFilter: service_description ~ Check_MK\nFilter: service_state = 3\n"+\
	    #			"And: 2\nOutputFormat: python\n"
            device_down_query = "GET services\nColumns: host_name\nFilter: service_description ~ Check_MK\nFilter: service_state = 3\nFilter: service_state = 2\nOr: 2\n"+\
                                "And: 2\nOutputFormat: python\n"

            #service_perf_query = "GET services\nColumns: host_name host_address service_description service_state "+\
            #                "last_check service_last_state_change host_state service_perf_data\nFilter: service_description ~ _invent\n"+\
            #                "Filter: service_description ~ _status\nFilter: service_description ~ Check_MK\nOr: 3\nNegate:\nOutputFormat: python\n"
            nw_qry_output = eval(get_from_socket(site_name, network_perf_query))
            serv_qry_output = eval(get_from_socket(site_name, service_perf_query))
            mrc_qry_output = eval(get_from_socket(site_name, mrc_query))
            device_down_output = eval(get_from_socket(site_name, device_down_query))
	    device_down_list =[str(item) for sublist in device_down_output for item in sublist]
	    for index in range(0,len(mrc_qry_output),2):
            	try:
                        if mrc_qry_output[index][0] == mrc_qry_output[index+1][0] and mrc_qry_output[index][2] != 1:
                                if mrc_qry_output[index][4] == mrc_qry_output[index+1][4]:
                                        mrc_hosts.append(mrc_qry_output[index][0])
                except:
                        continue

  	    # Code has been added to take average value of last 10 entries if device is not down and still unknown values
	    # comes because of packet lost in network for service.
	    #unknown_svc_data = filter(lambda x:(x[3] == 3) or ((st - pivot_timestamp_fwd(datetime.fromtimestamp(x[4]))) >= timedelta(minutes=4)),serv_qry_output)
	    #unknwn_state_svc_data = filter(lambda x: x[0] not in device_down_list ,unknown_svc_data)
	    #print '............................................'
            #print unknwn_state_svc_data
	    #print '............................................'
	    print len(serv_qry_output)
	    frequency_based_service_list =['wimax_ss_ip','wimax_modulation_dl_fec','wimax_modulation_ul_fec','wimax_dl_intrf',
		'wimax_ul_intrf','wimax_ss_sector_id','wimax_ss_mac','wimax_ss_frequency','mrotek_e1_interface_alarm',
		'mrotek_fe_port_state','mrotek_line1_port_state','mrotek_device_type','rici_device_type','rici_e1_interface_alarm',
		'rici_fe_port_state','rici_line1_port_state']
	    uptime_service =['wimax_ss_session_uptime','wimax_ss_uptime',
		'wimax_bs_uptime','cambium_session_uptime_system','cambium_uptime','radwin_uptime']

	    #unknwn_state_svc_data  =  calculate_avg_value(unknwn_state_svc_data,frequency_based_service_list,db)
	    unknwn_state_svc_data ={}
	    #build_export(memc_obj,
            #           site_name,
            #           nw_qry_output,
            #           serv_qry_output,mrc_hosts,device_down_list,unknwn_state_svc_data,
            #           db
            #)
	    #elapsed = int(time.time()) - current

        except SyntaxError, e:
            raise MKGeneralException(("Can not get performance data: %s") % (e))
        except socket.error, msg:
            raise MKGeneralException(("Failed to create socket. Error code %s Error Message %s:") % (str(msg[0]), msg[1]))
        except ValueError, val_err:
                print 'Error in serv/nw qry output'
                print val_err.message

def get_from_socket(site_name, query):
	"""
        Function_name : get_from_socket (collect the query data from the socket)

        Args: site_name (poller on which monitoring data is to be collected)

        Kwargs: query (query for which data to be collectes from nagios.)

        Return : None

        raise
             Exception: SyntaxError,socket error
    	"""
	socket_path = "/omd/sites/%s/tmp/run/live" % site_name
	#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
	machine = site_name[:-8]
	#socket_ip = _LIVESTATUS[machine]['host']
	#socket_port = _LIVESTATUS[machine][site_name]['port']
	#s.connect((socket_ip, socket_port))
	s.connect(socket_path)
	s.settimeout(60.0)
	s.send(query)
	s.shutdown(socket.SHUT_WR)
	output = ''
	while True:
		try:
			out = s.recv(100000000)
			out.strip()
	     	except socket.timeout,e:
			err=e.args[0]
			print 'socket timeout ..Exiting'
			if err == 'timed out':
				sys.exit(1) 
		
	     		if not len(out):
				break
	     	output += out

	return output


extract_data = BranchPythonOperator(
    task_id="ExtractData",
    provide_context=False,
    python_callable=connect_and_extract,
    dag=main_service_dag)


