import logging
import tempfile
import csv
import os
import traceback 
import time 
from airflow.hooks import MemcacheHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import MySqlHook
from airflow.hooks import RedisHook

class TelradExtractor(BaseOperator):
	"""
	This is a telrad operator which executes the provided query on the given connection name and then extracts the output and stores in 
	redis with timestamp and Data
	"""

	template_fields = ('sql',)
	template_ext = ('.sql',)
	ui_color = '#edffed'
	arguments= "self,query,telrad_conn_id,redis_conn_id, *args, **kwargs"

	@apply_defaults
	def __init__(
			self,query,telrad_conn_id,redis_conn_id, *args, **kwargs):
		super(MemcToMySqlOperator, self).__init__(*args, **kwargs)
		self.query=query
		self.telrad_conn_id = telrad_conn_id
		self.redis_conn_id = redis_conn_id,

	def execute(self, context):
		 logging.info("Executing Telrad Extractor Operator")
		 
