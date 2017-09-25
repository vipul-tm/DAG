import logging
import tempfile
import csv
import os
import traceback 
import time 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import MySqlHook
from airflow.hooks import RedisHook

class MySqlLoaderOperator(BaseOperator):
	"""
	transfers memc or redis data to specific mysql table

	:param mysql_conn_id: reference to a specific mysql database
	:type mysql_conn_id: string
	:param sql: the sql code to be executed
	:type sql: Can receive a str representing a sql statement,
		a list of str (sql statements), or reference to a template file.
		Template reference are recognized by str ending in '.sql'
	"""


	@apply_defaults
	def __init__(self,query="",data="",redis_key="",redis_conn_id = "redis_hook_6",mysql_conn_id='mysql_uat',*args, **kwargs):
		super(MySqlLoaderOperator, self).__init__(*args, **kwargs)
		self.mysql_conn_id = mysql_conn_id
		self.sql = query
		self.data = data
		self.redis_key = redis_key			 
		self.redis_hook = RedisHook(redis_conn_id = redis_conn_id)
		self.redis_conn_id = redis_conn_id
	def execute(self,context):
		hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
		if self.data=="" and self.redis_key != "" and self.redis_conn_id != "":
			
			self.data = eval(self.redis_hook.get(self.redis_key))
			#print type(self.data),len(self.data),self.data[0],type(self.data[0])
			if len(self.data) <=0:
				logging.error("Not inserting data as the provided key is empty")
				return 1
		try:
		    #print "Started Exec"
		    conn = hook.get_conn()
		    cursor = conn.cursor()
		    cursor.executemany(self.sql, self.data)
		except Exception,e:
		    logging.info("Exception")
		    traceback.print_exc()
		conn.commit()
		cursor.close()
		conn.close()
