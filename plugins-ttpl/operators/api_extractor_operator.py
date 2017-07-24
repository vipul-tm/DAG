import logging
import tempfile
import csv
import os
import traceback 
import time 
from airflow.hooks import MemcacheHook
from airflow.models import BaseOperator
from hooks.sshparamiko_extractor_hook import SSHParmikoHook
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook
import socket
import paramiko
import sys
import re
import random
import requests, json

class ApiExtractor(BaseOperator):
	"""
	This is a API operator which executes the provided API on the given connection name and then extracts the JSON output and stores in 
	redis with timestamp and Data<br />

	<b>Requirement</b> :API Connections and Query Connection <br /> 

	The Telrad connection should have: <br />
		1) URL: The api to fetch data from<br />
		2) api_params: API Params Dict <br />
		3) redis_conn_id: redis connection id string<br />
		7) identifier: The identifier from which data is to be accessed by other operators <br />

		"""

	
	ui_color = '#edffed'
	arguments= """
	The Telrad connection should have: <br />
		1) URL<br />
		2) api_params: <br />
		3) redis_conn_id: <br />
		4) identifier<br />

		
	"""

	@apply_defaults
	def __init__(
			self,url,api_params,request_type,headers,redis_conn_id,identifier, *args, **kwargs):
		super(ApiExtractor, self).__init__(*args, **kwargs)
		self.url=url
		self.api_params = api_params
		self.redis_conn_id = redis_conn_id
		
		self.request_type = request_type
		self.headers = headers
	
	def extract(self,request):	
		
		if request == "get":
			r = requests.get(self.url,headers=self.headers,params=self.api_params)
		elif request == "post":
			r = requests.post(self.url)

		data = r.json()
		return data



	def execute(self, context):
		data = 	self.extract(self.request_type)
		logging.info(data)
		timestamp= int(time.time())
		payload_dict = {
		"DAG_name":context.get("task_instance_key_str").split('_')[0],
		"task_name":context.get("task_instance_key_str"),
		"payload":data,
		"timestamp":timestamp
		}
		redis = RedisHook(redis_conn_id = "redis")
		conn = redis.get_conn()
		redis.add_event(identifier,timestamp,payload_dict)
