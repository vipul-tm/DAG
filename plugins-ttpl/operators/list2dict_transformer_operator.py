import logging
import traceback 
import time 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook

class List2DictTransformer(BaseOperator):
	"""
	This Operator is used to evaluate List data and converts the input data to dict provided as skeleton dict <br />

	<b> Requirement </b> :- <br />

	Connection:The Telrad connection should have: <br />
		1) redis_conn : the Redis Hook Object <br />
		2) identifier_input <br />
		3) identifier_output <br />
		4) output_identifier_index <br />
		5) start_timestamp <br />
		6) end_timestamp <br />
		7) payload
		8) index_key
		9) skeleton_dict
		10) indexed = False <br />

		"""

	
	ui_color = '#e1	ffed'
	arguments= """
		1) redis_conn : the Redis Hook Object <br />
		2) identifier_input <br />
		3) identifier_output <br />
		4) output_identifier_index <br />
		5) start_timestamp <br />
		6) end_timestamp <br />
		7) payload <br />
		8) index_key <br />
		9) skeleton_dict <br />
		10) indexed = False <br />

		"""

	@apply_defaults
	def __init__(
			self,redis_conn,identifier_input,identifier_output,output_identifier_index,start_timestamp,end_timestamp,payload,index_key,skeleton_dict,indexed=False, *args, **kwargs):
		super(List2DictTransformer, self).__init__(*args, **kwargs)
		print kwargs,BaseOperator
		self.redis_conn=redis_conn
		self.identifier_input = identifier_input
		self.start_timestamp = start_timestamp
		self.end_timestamp = end_timestamp
		self.identifier_output = identifier_output
		self.output_identifier_index = output_identifier_index
		self.payload = payload
		self.index_key = index_key
		self.indexed = indexed
		
	def execute(self, context,**kwargs):
		logging.info("Executing Evaluator Operator")
		transformed_data = []
		converted_dict = {}
		if indexed:
			data = redis_conn.get_event_by_key(self.identifier_input,self.payload,self.index_key,self.start_timestamp,self.end_timestamp)
		else:
			data = redis_conn.get_event(self.identifier_input,self.start_timestamp,self.end_timestamp)

		for data_values in data:
			converted_dict = {}
			for key in skeleton_dict:
				converted_dict[key] = data_values[int(skeleton_dict.get(key))]
				transformed_data.append(converted_dict.copy())

		if indexed:
			redis_conn.add_event_by_key(self.identifier_output,transformed_data,{self.output_identifier_index:self.output_identifier_index})
		else:
			redis_conn.add_event(self.identifier_input,self.start_timestamp,transformed_data)

		