import logging
import traceback 
import time 
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks import RedisHook
#from airflow.hooks import KafkaHook
import numpy as np
import cPickle
from airflow.hooks import MySqlHook
from airflow.models import Variable

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
        #self.hook = KafkaHook(kafka_conn_id = kafka_con_id)
        self.redis_hook = RedisHook(redis_conn_id="redis_hook_4")
        self.mysql_hook = MySqlHook(mysql_conn_id='mysql_uat')
    
    def insert_mysql(self,sql,mysql_data):
        conn = self.mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.executemany(sql, mysql_data)
        conn.commit()
        cursor.close()
        conn.close()


    def execute(self, context,**kwargs):
        network_dict = {
                'site': 'unknown' ,
                'host': 'unknown',
                'service': 'unknown',
                'ip_address': 'unknown',
                'severity': 'unknown',
                'age': 'unknown',
                'ds': 'unknown',
                'cur': 'unknown',
                'war': 'unknown',
                'cric': 'unknown',
                'check_time': 'unknown',
                'local_timestamp': 'unknown' ,
                'refer':'unknown',
                'min_value':'unknown',
                'max_value':'unknown',
                'avg_value':'unknown',
                'machine_name':'unknown'
                }

        logging.info("Executing Kafka Extractor Operator")
        #messages = self.hook.consume(self.consumer,self.topic)
        #consumer = self.hook.get_kafka_consumer('ml_queue',consumer_group='ml_group')
        #d = consumer.consume(block=False)
        consumer =  self.redis_hook.rget("ml_queue_main")
        logging.info (consumer)
        logging.info (type(consumer))
        mysql_data = []
        for msg in consumer:
            if len(eval(msg)) == 18:
                print "loop "
                logging.info(msg)
                msg = eval(msg)
                ip_address = msg[-2]
                host_name = msg[-1]

                msg = msg[:-2]
               # msg = eval(msg)
                
                timestamp = time.time()
                payload_dict = {
                "DAG_name":context.get("task_instance_key_str").split('_')[0],
                "task_name":context.get("task_instance_key_str"),
                "payload":msg,
                "timestamp":timestamp
                }
                try:
                    data = np.array(msg)
            
                    f = open(Variable.get('ml_classifier_path'),"rb")
                    svr = cPickle.load(f)
                    prediction = svr.predict(data)
                    prediction = float(prediction[0])
                except Exception:
                    logging.info("Ohhh Unable to predict , error in prediction code")
                    prediction = ''
                if prediction != '' and prediction != None:
                    if int(prediction) < 0:
                        prediction = 0
                    
                    network_dict['site'] = 'ospf1_slave_1'
                    network_dict['host'] = host_name
                    network_dict['service'] = 'ping'
                    network_dict['ip_address'] = ip_address
                    network_dict['severity'] = 'ok' if (prediction >=0 and prediction <= 10) else ('warning' if prediction > 10 and prediction <= 50 else 'critical')
                    network_dict['age'] = ''
                    network_dict['ds'] = 'pl'
                    network_dict['cur'] = prediction
                    network_dict['war'] = 10
                    network_dict['cric'] = 50
                    network_dict['check_time'] = int(time.time())
                    network_dict['local_timestamp'] = int(time.time())
                    network_dict['refer'] = ''
                    network_dict['min_value'] = 0
                    network_dict['max_value'] = 100
                    network_dict['avg_value'] = 50
                    network_dict['machine_name'] = 'ospf1'
                    mysql_data.append(network_dict.copy())
                
                    
                    #redis_conn.add_event(self.identifier_output,timestamp,payload_dict)

            else:
                print "Len is %s Dic is %s"%(len(eval(msg)),msg)
                continue
        query_nw = "INSERT INTO nocout_ospf1.performance_performancenetwork_predicted "
        query_nw +="(machine_name,current_value,service_name,avg_value,max_value,age,min_value,site_name,data_source,critical_threshold,device_name,severity,sys_timestamp,ip_address,warning_threshold,check_timestamp,refer ) values \
        (%(machine_name)s,%(cur)s,%(service)s,%(avg_value)s,%(max_value)s,%(age)s,%(min_value)s,%(site)s,%(ds)s,%(cric)s,%(host)s,%(severity)s,%(local_timestamp)s,%(ip_address)s,%(war)s,%(check_time)s,%(refer)s)"  

        self.insert_mysql(query_nw,mysql_data)

    
