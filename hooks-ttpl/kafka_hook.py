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
import pykafka
from pykafka import KafkaClient

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
	self.cnx = KafkaClient('localhost:9092')

    def get_conn_obj(self):
        conn_config = {
            "host": self.conn.host or '10.133.12.163',
            "topic": self.conn.schema or 'default'
        }

        if not self.conn.port:
            conn_config["port"] = 9092
        else:
            conn_config["port"] = int(self.conn.port)
        return conn_config

    def get_kafka_consumer(self,topic,consumer_group=None):
        #self.consumer_group = consumer_group
        print topic,consumer_group
        try:
            topic_instance = self.cnx.topics[topic]
            print topic_instance
            if topic_instance is not None:
                s = topic_instance.get_balanced_consumer(consumer_group,
                        auto_commit_enable=True, reset_offset_on_start=True)
                return s
        except Exception,e:
            print "Error in getting consumer from topic,Invalid Topic"
            print repr(e)
        print "Nitin after consumer"

    def get_kafka_producer(self,topic):
	print "In kafka producer"
        try:
            topic_instance = self.cnx.topics[topic]
            if topic_instance is not None:
                prod = topic_instance.get_producer()
		return prod
        except Exception,e:
	    print 'Error in getting producer'
            print e



    """

    def get_kafka_producer(self):
        conn_config = self.get_conn_obj()
        #print conn_config
        producer = KafkaProducer(bootstrap_servers=conn_config.get('host')+":"+str(conn_config.get('port')))
        #producer = KafkaProducer(bootstrap_servers='localhost:2181')
        return producer

    def get_kafka_consumer(self,topic):
        conn_config = self.get_conn_obj()
        kafka = KafkaClient(str(conn_config.get('host'))+":"+str(conn_config.get('port')))
        #kafka = KafkaClient(str('localhost')+":"+str('2181'))
        
        consumer = KafkaConsumer(topic)
        return consumer
        
    """
	#to set redis ey
    def publish(self,producer,topic,value):
        producer.send(topic, value)    
	#to get msg on topic
    
    def consume(self,consumer,topic):
        messages = []
        for msg in consumer:
            messages.append(msg)

        return messages

