###############Utility functions for format################

import re
from airflow.models import Variable
from os import listdir
from os.path import isfile, join
import time
import logging
import traceback
import math
from airflow.hooks import  MemcacheHook
from airflow.hooks import RedisHook

redis_hook_5 = RedisHook(redis_conn_id="redis_hook_5")
memc_con = MemcacheHook(memc_cnx_id = 'memc_cnx')
hostmk = Variable.get("hostmk.dict")
hostmk = eval(hostmk)
def get_threshold(perf_data):
    """
    Function_name : get_threshold (function for parsing the performance data and storing in the datastructure)

    Args: perf_data performance_data extracted from rrdtool

    Kwargs: None
    return:
           threshold_values (data strucutre containing the performance_data for all data sources)
    Exception:
           None
    """

    threshold_values= {}
    perf_data_list = filter(None,perf_data.split(";"))
    next_two = False
    next_items = 0
    data_source_name = ""
    for element in perf_data_list:
         if "=" in element:
            splitted_val = element.split("=")
            splitted_val[1] = splitted_val[1].strip("ms") if "ms" in splitted_val[1] else splitted_val[1]
            splitted_val[1] = splitted_val[1].strip("%") if "%" in splitted_val[1] else splitted_val[1]
            data_source_name = splitted_val[0].strip()
            threshold_values[data_source_name] = {
            'cur': splitted_val[1] 
            }
            next_items = 2
            next_two = True
         elif next_two: 
            if next_items > 0:
                if next_items == 2:
                    threshold_values[data_source_name].update({'war': element})
                elif next_items == 1:
                    threshold_values[data_source_name].update({'cric': element})
                next_items = next_items - 1
            else:
                next_two = False    
    
    return threshold_values

def get_device_type_from_name(hostname):
    try:
        device_type = hostmk.get(hostname)
        if device_type:
            return device_type
        else:
            logging.info("Device type for %s not found"%hostname)
            return 'unknown'
    except Exception:
        return 'unknown'
def create_file_and_write_data(name,data,dire = Variable.get("temp_dir_path")):
    file_to_write_to = open(str(dire)+str(name),"wb+")
    file_to_write_to.write(data)
    file_to_write_to.close()

def delete_files(name,dire = Variable.get("temp_dir_path")):
    try:
        os.remove(str(dire)+str(name))
    except Exception:
        logging.info("Unable to delete files "+ str(dire)+str(name))

def get_all_files_in_folder(dire = Variable.get("temp_dir_path") ):
    onlyfiles = [f for f in listdir(dire) if isfile(join(dire, f))]
    return onlyfiles


def get_previous_device_states(redis_hook_5,ds="pl"):
        all_devices_state = []
        try:
            if ds == "pl":
                all_devices_state = redis_hook_5.get("all_devices_state")
                if len(all_devices_state) > 0:
                    return eval(all_devices_state)
                else:
                    return None
            elif ds == "rta":
                all_devices_state = redis_hook_5.get("all_devices_state_rta")
                if len(all_devices_state) > 0:
                    return eval(all_devices_state)
            elif ds == "down":
                all_devices_down_state = redis_hook_5.get("all_devices_down_state")
                if len(all_devices_down_state) > 0:
                    return eval(all_devices_down_state)

                else:
                    return None

        except TypeError:
            return None
        except Exception:
            logging.error("Unable to get previous device states , Refer would be unknown ")
            traceback.print_exc()


def create_device_state_data():
    all_data = eval(Variable.get("hostmk.dict")) 
    if len(all_data) > 0:
        device_state_dict = {}

        for device in all_data.keys():
            device_state_dict[device] = {'state':'up',
                                        'since': int(time.time())
                                            }            
        redis_hook_5.set("all_devices_state",str(device_state_dict))
        logging.info("We have created the dictionary for the first time ")
    else:
        logging.info("No data Recieved from Extract data")


def forward_five_min(x):
        if x != None:
            value= int(math.ceil(int(x) / 300.0)) * 300
        else:
            value = 0
        return value

def forward_two_min(x):
        value= int(math.ceil(x / 120.0)) * 120
        return value

def backtrack_two_min(x):
        value= int(math.floor(x / 120.0)) * 120
        return value

def backtrack_x_min(x,seconds):
        value= int(math.floor(x / seconds)) * seconds
        return value

def memcachelist(key,value,memc_obj=memc_con):
        old_memc_value = memc_obj.get(str(key))
        try:
                if not old_memc_value:
                        old_memc_value = []
                if len(old_memc_value)< 2:
                        old_memc_value.append(value)

                elif len(old_memc_value) == 2:
                        old_memc_value.pop(0)
                        old_memc_value.append(value)
                memc_obj.set(key, old_memc_value)
        except Exception as e :
                print e
