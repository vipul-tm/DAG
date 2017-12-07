# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from redis import StrictRedis

from airflow.hooks.base_hook import BaseHook
import ujson # for zadd_compress
import zlib # for zadd_compress

class RedisHook(BaseHook):
	"""
	Interact with Redis.
	"""

	conn_name_attr = 'redis_conn_id'
	default_conn_name = 'redis_default'
	supports_autocommit = True

	def __init__(self, redis_conn_id = 'redis'):
		#super(RedisHook, self).__init__(*args, **kwargs)
		#self.schema = kwargs.pop("schema", None)
		self.redis_conn_id = redis_conn_id
		self.conn = self.get_connection(redis_conn_id)
		
	def get_conn(self):
		"""
		Returns a redis connection object
		"""
		conn_config = {
			"host": self.conn.host or 'localhost',
			"db": self.conn.schema or ''
		}

		if not self.conn.port:
			conn_config["port"] = 6379
		else:
			conn_config["port"] = int(self.conn.port)

		conn = StrictRedis(**conn_config)
		return conn

	#to set redis ey
	def set(self,key,value):
		conn = self.get_conn()
		conn.set(key,value)
		conn.connection_pool.disconnect()
	#to get setted redis key
	def get(self,key):
		conn = self.get_conn()
		conn.connection_pool.disconnect()
		return conn.get(key)
		
	#to push list into redis
	def rpush(self,key,value):
		conn = self.get_conn()
		conn.rpush(key,*value)
		conn.connection_pool.disconnect()
	#to pull all list from redis
	def rget(self,key):
		conn = self.get_conn()
		conn.connection_pool.disconnect()
		return conn.lrange(key, 0, -1)
		
	#to delete all matching keys
	def flushall(self,key):
		conn = self.get_conn()
		for key in conn.scan_iter(key):
			conn.delete(key)
		conn.connection_pool.disconnect()
	#to get all matching keys

	def get_keys(self,key):
	   conn = self.get_conn()
	   conn.connection_pool.disconnect()
	   return conn.scan_iter(key)
	   

	def hgetall(self,key):
	   conn = self.get_conn()
	   conn.connection_pool.disconnect()
	   return conn.hgetall(key)
	   

	def add_event_by_key(conn, identifier, events, index_keys={"search_key":"timestamp"}):
		"""
			This function is used to add the list of distionary into the Redis and index the data based on the dict key provided

		"""
		pipe = conn.pipeline(True)
		for event in events:
			id_redis = conn.incr('%s:id'%(identifier))
			event['id'] = id_redis
			event_key = '%s:%s'%(identifier,id_redis)
			pipe.hmset(event_key, event)
			#pipe.zadd(identifier,id_redis,event['timestamp'])
			for index_key in index_keys:
				pipe.zadd(index_key,event[index_keys[index_key]],id_redis)
		pipe.execute()
		conn.connection_pool.disconnect()
		return True

	def get_event_by_key(conn,identifier,index_keys,start_time,end_time):
		"""
			This function is used to get data from the specified identifier and key
		"""
		pipe = conn.pipeline(True)


	def add_event(conn,measurement_name,time,value):
		"""
		Add measurement metrics into redis db with timestamp labeled on them 

		"""
		try:
			conn.zadd(measurement_name,time,value)
			conn.connection_pool.disconnect()
			return True
		except Exception,e:
			conn.connection_pool.disconnect()
			print e
	def get_event(conn,measurement_name,start_time,end_time):
		"""
		Get measurement metrics into redis db with timestamp labeled on them 

		"""
		try:
			data = conn.zrangebyscore(measurement_name,start_time,end_time)
			conn.connection_pool.disconnect()
			return data
		except Exception,e:
			conn.connection_pool.disconnect()
			print e

	def zadd_compress(self,set_name,time,data_value):
	   """ adds data to redis on sorted set """
	   try:
		   conn=self.get_conn()
		   serialized_value = ujson.dumps(data_value)
		   compressed_value =zlib.compress(serialized_value)
		   conn.zadd(set_name,time,compressed_value)
		   conn.connection_pool.disconnect()
	   except Exception,exc:
		   print "Exception in zadd_compress ",exc
		   conn.connection_pool.disconnect()
		   #error('Redis error in adding in set{0}'.format(exc))

	def multi_set(conn, data_values, perf_type=''):
		""" Sets multiple key values through pipeline"""
		KEY = '%s:%s:%s' % (perf_type, '%s', '%s')
		pipe = conn.pipeline(transaction=True)
		# keep the provis data keys with a timeout of 5 mins
		[pipe.setex(KEY %
				 (d.get('device_name'), d.get('service_name')),
				 300, d.get('current_value')) for d in data_values
		 ]
		try:
			pipe.execute()
			conn.connection_pool.disconnect()
		except Exception as exc:
			conn.connection_pool.disconnect()
			print "Exception in multi_set ",exc
			#error('Redis pipe error in multi_set: {0}'.format(exc))

	def redis_update(conn, data_values, update_queue=False, perf_type=''):
		""" Updates multiple hashes in single operation using pipelining"""
		KEY = '%s:%s:%s:%s' % (perf_type, '%s', '%s', '%s')
		p = conn.pipeline(transaction=True)
		try:
			# update the queue values
			if update_queue:
				KEY = '%s:%s:%s' % (perf_type, '%s', '%s')
			   	devices = [(d.get('device_name'), d.get('service_name'))for d in data_values]
			   	# push the values into queues
			   	[p.rpush(KEY % (d.get('device_name'), d.get('service_name')), d.get('severity')) for d in data_values]
				# perform all operations atomically
				p.execute()
				# calc queue length corresponds to every host entry
				[p.llen(KEY % (x[0], x[1])) for x in devices]
			   	queues_len = p.execute()
				#host_queuelen_map = zip(devices, queues_len)
				# keep only 2 latest values for each host entry, if any
				#trim_hosts = filter(lambda x: x[1] > 2, host_queuelen_map)
				trim_hosts = []
				for _, x in enumerate(zip(devices, queues_len)):
					if x[1] > 2:
						trim_hosts.append(x[0])
						[p.ltrim(KEY % (x[0], x[1]), -2, -1) for x in trim_hosts]
				# update the hash values
					else:
						[p.hmset(KEY %(d.get('device_name'), d.get('service_name'), d.get('data_source')),d) for d in data_values]
						# perform all operations atomically
				p.execute()
				conn.connection_pool.disconnect()
		except Exception as exc:
				#error('Redis pipe error in update... {0}, retrying...'.format(exc))
				conn.connection_pool.disconnect()
				print "Exception in redis_update ",exc
				# send the task for retry
				#Task.retry(args=(data_values), kwargs={'perf_type': perf_type},
				#			   max_retries=2, countdown=10, exc=exc)


