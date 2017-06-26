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
from airflow.models import Variable

class MemcToMySqlOperator(BaseOperator):
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
			self, sql,memc_key,mysql_table,exec_type=1,update=False,memc_conn_id = 'memc_cnx', mysql_conn_id='mysql_uat', parameters=None,
			autocommit=True,db_coloumns=None, *args, **kwargs):
		super(MemcToMySqlOperator, self).__init__(*args, **kwargs)
		self.mysql_conn_id = mysql_conn_id
		self.sql = sql
		self.memc_key = memc_key
		self.mysql_table = mysql_table
		self.memc_conn_id = memc_conn_id
		self.exec_type = exec_type
		self.autocommit = autocommit
		self.parameters = parameters
		self.memc_conn_id = memc_conn_id
		self.update = update
		self.db_coloumns = db_coloumns
		self.db = "_".join(memc_key.split("_")[2:4])
	def execute(self, context):
		debug_mode = eval(Variable.get("debug_mode"))
		if not debug_mode:
			start_main = time.time()
			
			hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
			memc_hook = MemcacheHook(memc_cnx_id = self.memc_conn_id)
			redis_hook_2 = RedisHook(redis_conn_id="redis_hook_4")
			keys=[]
			redis_data = []
			all_data = []
			try:
				logging.info("Now only getting data from redis on key %s"%(self.memc_key))
				redis_data = redis_hook_2.rget(self.memc_key)
				logging.info("Type of data recieved is "+str(type(redis_data)))
				#redis_data = eval(data) #assigning data from redis to memc variable

				if self.exec_type == 1:
					logging.info("Executing bulk upload over the provided connection")
					for val in redis_data:
						slot_data = eval(val)
						for device_data in slot_data:							
							device_data = eval(device_data)					
							all_data.append(device_data)
					redis_data = []
				else:
					logging.info("Executing executemany over the provided connection")
					for i,val in enumerate(redis_data):
						redis_data[i] = eval(redis_data[i])

				
				if (all_data != None and len(all_data) > 0) or (redis_data != None and len(redis_data) > 0):
					logging.info("checking dict keys")

					if self.exec_type == 1 and  len(all_data) > 0:
						keys = all_data[0].keys()
					else:
						keys = redis_data[0][0].keys()	
				else:
					logging.error("We did not recieved any data on KEY: "+ self.memc_key)
			except Exception:
				logging.info("Not able to get the data from Redis")
				traceback.print_exc()
			
				
			if len(all_data) > 0 or len(redis_data) > 0:
				if self.exec_type == 1:
					start_file = time.time()
					with open("/home/tmadmin/a.csv","wb+") as file:
						dict_writer = csv.DictWriter(file, keys,lineterminator=";\r\n",delimiter="|",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
						dict_writer.writeheader()
						try:
							dict_writer.writerows(all_data)
						except UnicodeEncodeError:
							logging.info("Got Unicode charater")
							
						file.close()			

					with tempfile.NamedTemporaryFile(delete=False) as temp:
						dict_writer = csv.DictWriter(temp,keys,lineterminator=";\n",delimiter="|",quoting=csv.QUOTE_MINIMAL,skipinitialspace=True)
						try:
							dict_writer.writerows(all_data)
						except UnicodeEncodeError:
							logging.info("Got Unicode charater")
						
						temp.close()
						start_file = time.time()	
						if self.update:
							query = "LOAD DATA LOCAL INFILE \'%s\' REPLACE INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
						else:
							query = "LOAD DATA LOCAL INFILE \'%s\' INTO TABLE %s.%s FIELDS TERMINATED BY '|' LINES TERMINATED BY ';' (%s);" %(temp.name,self.db,self.mysql_table,self.db_coloumns)
						hook.run(
							query,
							autocommit=self.autocommit,
							parameters=self.parameters)
					os.unlink(temp.name)	
					logging.info ("Time to Execute Bulk Upload= "+str(time.time()-start_file))
				else:
					logging.info('Executing: ' + str(self.sql))
					if not self.update:
						new_data = []
						new_slot = []

						for slot in redis_data:
							logging.info("Len of slot is ---> %s and len of redis_data is %s"%(len(slot),len(redis_data)) )
							new_slot = []
							for device in slot:
								device = eval(device)
								
								device['current_value'] = device.pop('cur').encode('ascii','ignore')
			
								device['service_name'] = str(device.pop('service'))
								device['site_name'] = str(device.pop('site')) 
								device['data_source'] = str(device.pop('ds').encode('ascii','ignore'))
								device['critical_threshold'] =  str(device.pop('cric'))
								device['device_name'] = str(device.pop('host'))
								device['warning_threshold'] = str(device.pop('war'))
								device['check_timestamp'] = int(device.pop('check_time'))
								device['sys_timestamp'] = int(device.pop('local_timestamp'))
								new_slot.append(device.copy())
							conn = hook.get_conn()
							cursor = conn.cursor()
							logging.info("We are about to upload %s service in total ."%len(new_data))
							try:
									cursor.executemany(self.sql, new_slot)
									logging.info("Successfully executed Query")
							except Exception:
									logging.info("Some Problem")
									traceback.print_exc()
							conn.commit()
							cursor.close()
							conn.close()
							#new_data.extend(new_slot)
						#logging.info(new_data[0])
						#conn = hook.get_conn()
						#cursor = conn.cursor()
						#logging.info("We are about to upload %s service in total ."%len(new_data))		
						#try:
						#	cursor.executemany(self.sql, new_data)
						#	logging.info("Successfully executed Query")
						#except Exception:
						#	logging.info("Some Problem")
						#	traceback.print_exc()
						#conn.commit()
						#cursor.close()
						#conn.close()

					else:

						new_data = []
						new_slot = []

						for slot in redis_data:
							logging.info("Len of slot is ---> %s and total slots are %s"%(len(slot),len(redis_data)) )
							new_slot = []
							for device in slot:
								device = eval(device)
								device['current_value'] = device.pop('cur')
								device['service_name'] = device.pop('service') 
								device['site_name'] = device.pop('site') 
								device['data_source'] = device.pop('ds')
								device['critical_threshold'] =  device.pop('cric')
								device['device_name'] = device.pop('host')
								device['warning_threshold'] = device.pop('war') 
								device['check_timestamp'] = int(device.pop('check_time'))
								device['sys_timestamp'] = int(device.pop('local_timestamp'))
								new_slot.append(device.copy())
							conn = hook.get_conn()
					   		cursor = conn.cursor()
							start_many = time.time()

							logging.info("We are about to update %s devices in total ."%len(new_data))

							try:
									cursor.executemany(self.sql,new_slot)
									logging.info("Successfully executed Query")
							except Exception:
									logging.info("Met Exception")
									traceback.print_exc()

							logging.info("We are about to update %s devices in total ."%len(new_data))
							conn.commit()
							cursor.close()
							conn.close()
							#new_data.extend(new_slot)

						
						#conn = hook.get_conn()
						#cursor = conn.cursor()
						#start_many = time.time() 

						#logging.info("We are about to update %s devices in total ."%len(new_data))
						
						#try:
						#	cursor.executemany(self.sql,new_data)
						#	logging.info("Successfully executed Query")
						#except Exception:
						#	logging.info("Met Exception")
						#	traceback.print_exc()

						#logging.info("We are about to update %s devices in total ."%len(new_data))
						#conn.commit()
						#cursor.close()
						#conn.close()
					
						logging.info ("Time to Execute Update Query = "+str(time.time()-start_many))
						logging.info ("Total Exec time = "+str(time.time() - start_main))
			else:
				logging.info("No Data recieved from memc or redis")
		else:
			logging.info("DEBUG MODE IS ACTIVE WILL NOT CHANGEDB TO DB")

