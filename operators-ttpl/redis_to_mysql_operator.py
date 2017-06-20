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
import json

class RedisToMySqlOperator(BaseOperator):
	"""
	transfers memc data to specific mysql table

	:param mysql_conn_id: reference to a specific mysql database
	:type mysql_conn_id: string
	:param sql: the sql code to be executed
	:type sql: Can receive a str representing a sql statement,
		a list of str (sql statements), or reference to a template file.
		Template reference are recognized by str ending in '.sql'
	"""

	template_fields = ('sql',)
	template_ext = ('.sql',)
	ui_color = '#ededed'

	@apply_defaults
	def __init__(
			self, sql,redis_key,mysql_table,exec_type=1,update=False,redis_conn_id = 'redis_hook_4', mysql_conn_id='mysql_uat', parameters=None,
			autocommit=True,db_coloumns=None, *args, **kwargs):
		super(RedisToMySqlOperator, self).__init__(*args, **kwargs)
		self.mysql_conn_id = mysql_conn_id
		self.sql = sql
		self.redis_key = redis_key
		self.mysql_table = mysql_table
		self.redis_conn_id = redis_conn_id
		self.exec_type = exec_type
		self.autocommit = autocommit
		self.parameters = parameters
		self.redis_conn_id = redis_conn_id
		self.update = update
		self.db_coloumns = db_coloumns
		self.db = "_".join(redis_key.split("_")[1:3])
	def execute(self, context):
		start_main = time.time()
		logging.info('Executing: ' + str(self.sql))
		hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
		redis_hook = RedisHook(self.redis_conn_id)

		try:
			logging.info("Getting Data from KEY : %s"%self.redis_key)
			try:
				redis_data = redis_hook.rget(self.redis_key)
			except Exception:
				redis_data = redis_hook.get(self.redis_key)
	

			if redis_data != None and len(redis_data) > 0:
				redis_data = eval(str(redis_data))
				#keys = redis_data[0][0].keys()	

				logging.info("Got the data from redis instead")
			else:
				logging.error("We did not recieved any data on KEY: "+ self.redis_key)
			
			
		except Exception:
			logging.info("Not able to get the data from Redis")
			traceback.print_exc()
			
	
			
		if len(redis_data) > 0:
			if self.exec_type == 1:
				start_file = time.time()
				# with open("/home/tmadmin/a.csv","wb+") as file:
				# 	dict_writer = csv.DictWriter(file, keys,lineterminator=";\r\n",delimiter="|",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
				# 	dict_writer.writeheader()
				# 	for slot_data in redis_data:
				# 		try:
				# 			dict_writer.writerows(slot_data)
				# 		except UnicodeEncodeError:
				# 			logging.info("Got Unicode charater")
				# 	file.close()			

				with tempfile.NamedTemporaryFile(delete=False) as temp:
					dict_writer = csv.DictWriter(temp,keys,lineterminator=";\n",delimiter="|",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
					for slot_data in redis_data:
						try:
							dict_writer.writerows(slot_data)
						except UnicodeEncodeError:
							logging.info("Got Unicode charater")
					
					temp.close()
							
					if self.update:
						query = "LOAD DATA LOCAL INFILE \'%s\' REPLACE INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
					else:
						query = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
			
					logging.info("================QUERY:   "+ query)
					hook.run(
						query,
						autocommit=self.autocommit,
						parameters=self.parameters)
				os.unlink(temp.name)	
				logging.info ("Time to Execute Bulk Upload= "+str(time.time()-start_file))
			elif self.exec_type == 3:
				logging.info("Generating Mysql Srtatement to find and delete rows which are present and then Insert New ones Intented for topology")
				#redis_data = eval(str(redis_data))
				for i,d in enumerate(redis_data):
					redis_data[i] = eval(d)

				hostnames = [str(d['device_name']) for d in redis_data]
				services = [str(d['service_name']) for d in redis_data]
		
				query = "delete from nocout_ospf1.%s where device_name in ('%s') and service_name in ('%s')"%(self.mysql_table,'\',\''.join(set(hostnames)),'\',\''.join(set(services)))
				hook.run(
						query,
						autocommit=self.autocommit,
						parameters=self.parameters)
				logging.info("success in executing delete ")
				logging.info("inserting new values")
				conn = hook.get_conn()
				cursor = conn.cursor()
				cursor.executemany(self.sql, redis_data)
				conn.commit()
				cursor.close()
				conn.close()
				logging.info("Successfully inserted in the DB")
			else:
				new_data = []
				new_slot = []

				for slot in redis_data:
					for device in slot:
						device['current_value'] = device.pop('cur')
						device['service_name'] = device.pop('service') 
						device['site_name'] = device.pop('site') 
						device['data_source'] = device.pop('ds')
						device['critical_threshold'] =  device.pop('cric')
						device['device_name'] = device.pop('host')
						device['warning_threshold'] = device.pop('war') 
						device['check_timestamp'] = device.pop('check_time')
						device['sys_timestamp'] = device.pop('local_timestamp')
					new_slot.append(device.copy())
				new_data.append(new_slot)

				start_many = time.time()
				conn = hook.get_conn()
				cursor = conn.cursor()
				
				start_many = time.time() 
				for slot in new_data:
						try:
							cursor.executemany(self.sql, slot)
						except Exception:
							traceback.print_exc()
				conn.commit()
				cursor.close()
				conn.close()
				logging.info("records Inserted "+ str(len(redis_data[0])))
				logging.info ("Time to Execute Insert Query = "+str(time.time()-start_many))
				logging.info ("Total Exec time = "+str(time.time() - start_main))
		else:
			logging.info("No Data recieved from redis or redis") 
