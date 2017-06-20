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
		return conn.get(key)
		conn.connection_pool.disconnect()
	#to push list into redis
	def rpush(self,key,value):
		conn = self.get_conn()
		conn.rpush(key,*value)
		conn.connection_pool.disconnect()
	#to pull all list from redis
	def rget(self,key):
		conn = self.get_conn()
		return conn.lrange(key, 0, -1)
		conn.connection_pool.disconnect()
	#to delete all matching keys
	def flushall(self,key):
		conn = self.get_conn()
		for key in conn.scan_iter(key):
			conn.delete(key)
		conn.connection_pool.disconnect()
	#to get all matching keys

	def get_keys(self,key):
	   conn = self.get_conn()
	   return conn.scan_iter(key)
	   conn.connection_pool.disconnect()

	def hgetall(self,key):
	   conn = self.get_conn()
	   return conn.hgetall(key)
	   conn.connection_pool.disconnect()
	
