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
from kafka import KafkaClient
from kafka import KafkaProducer
from kafka import KafkaConsumer


from airflow.hooks.base_hook import BaseHook


class KafkaHook(BaseHook):
    """
    Interact with Kafka.
    """

    conn_name_attr = 'kafka_conn'
    default_conn_name = 'kafka_default'
    supports_autocommit = True

    def __init__(self, kafka_conn_id = default_conn_name):
        #super(RedisHook, self).__init__(*args, **kwargs)
        #self.schema = kwargs.pop("schema", None)
        self.kafka_conn_id = kafka_conn_id
        self.conn = self.get_connection(kafka_conn_id)


    def get_conn_obj(self):
        conn_config = {
            "host": self.conn.host or 'localhost',
            "topic": self.conn.schema or 'default'
        }

        if not self.conn.port:
            conn_config["port"] = 9092
        else:
            conn_config["port"] = int(self.conn.port)
        return conn_config


    def get_kafka_producer(self):
        """
        Returns a kafka producer object
        """
        conn_config = self.get_conn_obj()
        producer = KafkaProducer(bootstrap_servers=conn_config.get('hosts')+":"+conn_config.get('port'))
        return producer

    def get_kafka_consumer(self,topic):
        """
        Returns a kafka consumer object
        """
        conn_config = self.get_conn_obj()
        kafka = KafkaClient(conn_config.get('host')+":"+str(conn_config.get('port')))
        
        consumer = KafkaConsumer(topic)
        return consumer
        

	#to set redis ey
    def publish(self,producer,topic,value):
        producer.send(topic, value)    
	#to get msg on topic
    
    def consume(self,consumer,topic):
        messages = []
        for msg in consumer:
            messages.append(msg)

        return messages

