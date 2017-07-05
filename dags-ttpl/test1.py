#from ospf1_slave_1 import *
import os
import json
from pprint import pformat
import re
from datetime import datetime, timedelta
import subprocess
import sys
import time
import socket
from collections import defaultdict
#from handlers.db_ops import *
from itertools import groupby
from operator import itemgetter




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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    machine = site_name[:-8]
    #socket_ip = _LIVESTATUS[machine]['host']
    #socket_port = _LIVESTATUS[machine][site_name]['port']
    s.connect(('10.133.19.165', 6561))
    #s.connect(socket_path)
    s.settimeout(60.0)
    s.send(query)
    s.shutdown(socket.SHUT_WR)
    output = ''
    while True:
     try:
        out = s.recv(100000000)
     except socket.timeout,e:
        err=e.args[0]
        print 'socket timeout ..Exiting'
        if err == 'timed out':
                sys.exit(1)
     out.strip()
     if not len(out):
        break
     output += out

    return output

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
q3 = "GET services\nColumns: host_name host_address host_state service_description service_state plugin_output\n" + \
                "Filter: service_description = wimax_topology\nOutputFormat: json\n"
site_name = 'ospf1_slave_1'
nw_qry_output = eval(get_from_socket(site_name, service_perf_query)) 
print nw_qry_output[0]

