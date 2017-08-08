import logging
import traceback 
import time 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook
from airflow.hooks import KafkaHook
class KafkaExtractorOperator(BaseOperator):
    """
    This Operator is used to evaluate List data and converts the input data to dict provided as skeleton dict <br />

    <b> Requirement </b> :- <br />

    Connection:The Telrad connection should have: <br />
        1) redis_conn : the Redis Hook Object <br />
        2) kafka_con_id <br />
        2) topic <br />
        3) identifier_output <br />
        4) output_identifier_index <br />
        5) start_timestamp <br />
        6) end_timestamp <br />
        7) payload
        8) index_key
        10) indexed = False <br />

        """

    
    ui_color = '#e1 ffed'
    arguments= """
        1) redis_conn : the Redis Hook Object <br />
        2) kafka_conn_id <br />
        3) topic <br />
        4) identifier_output <br />
        5) output_identifier_index <br />
        8) index_key <br />
        10) indexed = False <br />

        """

    @apply_defaults
    def __init__(
            self,redis_conn,kafka_con_id,topic,identifier_output,output_identifier_index,index_key,indexed=False, *args, **kwargs):
        super(KafkaExtractorOperator, self).__init__(*args, **kwargs)
        print kwargs,BaseOperator
        self.redis_conn=redis_conn
        self.topic = topic
        self.kafka_con_id = kafka_con_id
        self.identifier_output = identifier_output
        self.output_identifier_index = output_identifier_index
        self.index_key = index_key
        self.indexed = indexed
        self.hook = KafkaHook(kafka_conn_id = kafka_con_id)


    def execute(self, context,**kwargs):
        logging.info("Executing Kafka Extractor Operator")
        #messages = self.hook.consume(self.consumer,self.topic)
        consumer = self.hook.get_kafka_consumer('msg_q')
        #d = self.hook.consume(consumer,self.topic)
        for msg in consumer:
            logging.info(msg)
            timestamp = time.time()
            payload_dict = {
            "DAG_name":context.get("task_instance_key_str").split('_')[0],
            "task_name":context.get("task_instance_key_str"),
            "payload":msg,
            "timestamp":timestamp
            }
            if indexed:
                redis_conn.add_event_by_key(self.identifier_output,payload_dict,{self.output_identifier_index:self.output_identifier_index})
            else:
               
                redis_conn.add_event(self.identifier_output,timestamp,payload_dict)