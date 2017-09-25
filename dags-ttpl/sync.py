import os
import socket
from airflow import DAG
from airflow.contrib.hooks import SSHHook
from airflow.operators import PythonOperator
from airflow.operators import BashOperator
from airflow.operators import BranchPythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks import RedisHook
from airflow.hooks.mysql_hook import MySqlHook
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


#################################################################DAG CONFIG####################################################################################

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
PARENT_DAG_NAME = "SYNC"
main_etl_dag=DAG(dag_id=PARENT_DAG_NAME, default_args=default_args, schedule_interval='@once')
SQLhook=MySqlHook(mysql_conn_id='application_db')
redis_hook_2 = RedisHook(redis_conn_id="redis_hook_2")
#################################################################FUCTIONS####################################################################################
def get_host_ip_mapping():
    path = Variable.get("hosts_mk_path")
    try:
        host_var = load_file(path)
        ipaddresses = host_var.get('ipaddresses')
        return ipaddresses 
    except IOError:
        logging.error("File Name not correct")
        return None
    except Exception:
        logging.error("Please check the HostMK file exists on the path provided ")
        return None


def load_file(file_path):
    #Reset the global vars
    host_vars = {
        "all_hosts": [],
        "ipaddresses": {},
        "host_attributes": {},
        "host_contactgroups": [],
    }
    try:
        execfile(file_path, host_vars, host_vars)
        del host_vars['__builtins__']
    except IOError, e:
        pass
    return host_vars


def process_host_mk():
	path = Variable.get("hosts_mk_path")
	hosts = {}
	site_mapping = {}
	all_site_mapping =[]
	all_list = []
	device_dict = {}
	start = 0
	tech_wise_device_site_mapping = {}
	try:
		text_file = open(path, "r")
		
	except IOError:
		logging.error("File Name not correct")
		return "notify"
	except Exception:
		logging.error("Please check the HostMK file exists on the path provided ")
		return "notify"

	lines = text_file.readlines()
	host_ip_mapping = get_host_ip_mapping()
	for line in lines:
	    if "all_hosts" in line:
	  		start = 1

	    if start == 1:
	        hosts["hostname"] = line.split("|")[0]
	        hosts["device_type"] = line.split("|")[1]
	        site_mapping["hostname"] = line.split("|")[0].strip().strip("'")
	        
	        site_mapping['site'] = line.split("site:")[1].split("|")[0].strip()  

	        site_mapping['device_type'] = line.split("|")[1].strip()
	       
	        all_list.append(hosts.copy())
	        all_site_mapping.append(site_mapping.copy())
	        if ']\n' in line:
	        	start = 0
	        	all_list[0]['hostname'] = all_list[0].get("hostname").strip('all_hosts += [\'')
	        	all_site_mapping[0] ['hostname'] = all_site_mapping[0].get("hostname").strip('all_hosts += [\'')
	        	break
	print "LEN of ALL LIST is %s"%(len(all_list))
	if len(all_list) > 1:
	   	for device in all_list:
	  		device_dict[device.get("hostname").strip().strip("'")] = device.get("device_type").strip()
		Variable.set("hostmk.dict",str(device_dict))

		for site_mapping in all_site_mapping:
			if site_mapping.get('device_type') not in  tech_wise_device_site_mapping.keys():
				tech_wise_device_site_mapping[site_mapping.get('device_type')] = {site_mapping.get('site'):[{"hostname":site_mapping.get('hostname'),"ip_address":host_ip_mapping.get(site_mapping.get('hostname'))}]}

			else:
				if site_mapping.get('site') not in  tech_wise_device_site_mapping.get(site_mapping.get('device_type')).keys():
					tech_wise_device_site_mapping.get(site_mapping.get('device_type'))[site_mapping.get('site')] = [{"hostname":site_mapping.get('hostname'),"ip_address":host_ip_mapping.get(site_mapping.get('hostname'))}]
				else:
					tech_wise_device_site_mapping.get(site_mapping.get('device_type')).get(site_mapping.get('site')).append({"hostname":site_mapping.get('hostname'),"ip_address":host_ip_mapping.get(site_mapping.get('hostname'))})

		Variable.set("hostmk.dict.site_mapping",str(tech_wise_device_site_mapping))
		count = 0 
		for x in tech_wise_device_site_mapping:
			for y in tech_wise_device_site_mapping.get(x):
				count = count+len(tech_wise_device_site_mapping.get(x).get(y))\

		print "COUNT : %s"%(count)
		return 0
	else:
		return -4

def dict_rows(cur):
    desc = cur.description
    return [
        dict(zip([col[0] for col in desc], row))
        for row in cur.fetchall()
    ]


def execute_query(query):
	conn = SQLhook.get_conn()
	cursor = conn.cursor()
	cursor.execute(query)
	data =  dict_rows(cursor)
	cursor.close()
	return data

def createDict(data):
	#TODOL There are 3 levels of critality handle all those(service_critical,critical,dtype_critical)
	rules = {}
	ping_rule_dict = {}
	operator_name_with_operator_in = eval(Variable.get("special_operator_services")) #here we input the operator name in whcih we wish to apply IN operator
	service_name_with_operator_in = []
	for operator_name in operator_name_with_operator_in:
		service_name = "_".join(operator_name.split("_")[:-1])
		service_name_with_operator_in.append(service_name)


	for device in data:
		service_name = device.get('service')
		device_type = device.get('devicetype')
		if device.get('dtype_ds_warning') and device.get('dtype_ds_critical'):
			device['critical'] = device.get('dtype_ds_critical')
			device['warning'] = device.get('dtype_ds_warning')
		elif device.get('service_warning') and device.get('service_critical'):
			device['critical'] = device.get('service_critical')
			device['warning'] = device.get('service_warning')
		

		if service_name == 'radwin_uas' and device['critical'] == "":
			continue
			
		if service_name:
			name =  str(service_name)
			rules[name] = {}
		if device.get('critical'):
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than' if ("_rssi"  not in name) and ("_uas"  not in name) else "less_than_equal_to", 'value': device.get('critical') or device.get('dtype_ds_critical')}]}
		else:
			rules[name]={"Severity1":["critical",{'name': str(name)+"_critical", 'operator': 'greater_than', 'value': ''}]}
		if device.get('warning'):
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than' if ("_rssi"  not in name) and ("_uas"  not in name) else "less_than_equal_to" , 'value': device.get('warning') or device.get('dtype_ds_warning')}]})
		else:
			rules[name].update({"Severity2":["warning",{'name': str(name)+"_warning", 'operator': 'greater_than', 'value': ''}]})
		if device_type not in ping_rule_dict:
			if device.get('ping_pl_critical') and device.get('ping_pl_warning') and device.get('ping_rta_critical') and device.get('ping_rta_warning'):
				 ping_rule_dict[device_type] = {
				'ping_pl_critical' : device.get('ping_pl_critical'),
				'ping_pl_warning': 	 device.get('ping_pl_warning') ,
				'ping_rta_critical': device.get('ping_rta_critical'),
				'ping_rta_warning':  device.get('ping_rta_warning')
				}
	for device_type in  ping_rule_dict:
		if ping_rule_dict.get(device_type).get('ping_pl_critical'):
			rules[device_type+"_pl"]={}
			rules[device_type+"_pl"].update({"Severity1":["critical",{'name': device_type+"_pl_critical", 'operator': 'greater_than', 'value': float(ping_rule_dict.get(device_type).get('ping_pl_critical')) or ''}]})
			
		if ping_rule_dict.get(device_type).get('ping_pl_warning'):
			rules[device_type+"_pl"].update({"Severity2":["warning",{'name': device_type+"_pl_warning", 'operator': 'greater_than', 'value': float(ping_rule_dict.get(device_type).get('ping_pl_warning')) or ''}]})
			rules[device_type+"_pl"].update({"Severity3":["up",{'name': device_type+"_pl_up", 'operator': 'less_than', 'value': float(ping_rule_dict.get(device_type).get('ping_pl_warning')) or ''},'AND',{'name': device_type+"_pl_up", 'operator': 'greater_than_equal_to', 'value': 0}]})
			rules[device_type+"_pl"].update({"Severity4":["down",{'name': device_type+"_pl_down", 'operator': 'equal_to', 'value': 100}]})
			
		if ping_rule_dict.get(device_type).get('ping_rta_critical'):
			rules[device_type+"_rta"] = {}
			rules[device_type+"_rta"].update({"Severity1":["critical",{'name': device_type+"_rta_critical", 'operator': 'greater_than', 'value': float(ping_rule_dict.get(device_type).get('ping_rta_critical')) or ''}]})
		if ping_rule_dict.get(device_type).get('ping_rta_warning'):
			rules[device_type+"_rta"].update({"Severity2":["warning",{'name': device_type+"_rta_warning", 'operator': 'greater_than', 'value': float(ping_rule_dict.get(device_type).get('ping_rta_warning')) or ''}]})
			rules[device_type+"_rta"].update({"Severity3":["up",{'name': device_type+"_rta_up", 'operator': 'less_than', 'value': float(ping_rule_dict.get(device_type).get('ping_rta_warning'))},'AND',{'name': device_type+"_rta_up", 'operator': 'greater_than', 'value': 0 }]})
			
			#TODO: This is a seperate module should be oneto prevent re-looping ovver rules
	for rule in rules:
		if rule in set(service_name_with_operator_in):
			#service_name = "_".join(rule.split("_")[0:4])
			service_rules = rules.get(rule)
			for i in range(1,len(service_rules)+1):		
				severities = service_rules.get("Severity%s"%i)
				for x in range(1,len(severities),2):
					if severities[x].get("name") in operator_name_with_operator_in.keys():
						severities[x]["operator"] = operator_name_with_operator_in.get(severities[x].get("name"))

	return rules

def process_kpi_rules(all_services_dict):
	#TODO Update this code for both ul_issue and other KPIS
	kpi_rule_dict = {}
	formula_mapper = eval(Variable.get('ul_issue_kpi_to_formula_mapping'))
	kpi_services_mapper = eval(Variable.get('ul_issue_services_mapping'))
	kpi_services_mapper = eval(Variable.get('provision_services_mapping'))
	formula_mapper = eval(Variable.get('provision_kpi_to_formula_mapping'))
	for service in all_services_dict.keys():
		if "provis_kpi" in service:
			try:
				
				device_type = ""
				for device_type_loop in formula_mapper:
					if service in formula_mapper.get(device_type_loop):
						device_type = device_type_loop
	
				kpi_services = kpi_services_mapper.get(device_type)
		
				kpi_rule_dict[service] = {
				"name":service,
				"isFunction":False,
				"formula":"%s",
				"isarray":[False,False],
				"service":kpi_services,
				"arraylocations":0
				}
			except Exception:
				print service
				traceback.print_exc()
				continue
	print  kpi_rule_dict

	return kpi_rule_dict

def generate_service_rules():

	service_threshold_query = Variable.get('q_get_thresholds')
	#creating Severity Rules
	data = execute_query(service_threshold_query)
	rules_dict = createDict(data)
	Variable.set("rules",str(rules_dict))

#can only be done if generate_service_rules is completed and there is a rule Variable in Airflow Variables
def generate_kpi_rules():

	service_rules = eval(Variable.get('rules'))
	processed_kpi_rules = process_kpi_rules(service_rules)
	#Variable.set("kpi_rules",str(processed_kpi_rules))

def generate_kpi_prev_states():
	ul_tech = eval(Variable.get('ul_issue_kpi_technologies'))
	old_pl_data = redis_hook_2.get("all_devices_state")
	all_device_type_age_dict = {}
	for techs_bs in ul_tech:		
		redis_hook_2.set("kpi_ul_prev_state_%s"%(ul_tech.get(techs_bs)),old_pl_data)
		redis_hook_2.set("kpi_ul_prev_state_%s"%(techs_bs),old_pl_data)
##################################################################TASKS#########################################################################3
create_devicetype_mapping_task = PythonOperator(
    task_id="generate_host_devicetype_mapping",
    provide_context=False,
    python_callable=process_host_mk,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)
create_severity_rules_task = PythonOperator(
    task_id="generate_service_rules",
    provide_context=False,
    python_callable=generate_service_rules,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)
create_kpi_rules_task = PythonOperator(
    task_id="generate_kpi_rules",
    provide_context=False,
    python_callable=generate_kpi_rules,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)

create_kpi_prev_states = PythonOperator(
    task_id="generate_kpi_previous_states",
    provide_context=False,
    python_callable=generate_kpi_prev_states,
    #params={"redis_hook_2":redis_hook_2},
    dag=main_etl_dag)




##################################################################END#########################################################################3

