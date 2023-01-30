import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
import logging

logging.basicConfig(
         format='%(asctime)s %(levelname)-8s %(message)s',
         level=logging.INFO,
         datefmt='%Y-%m-%d %H:%M:%S')

Tracing_TOPIC = "trace_details"
consumer = KafkaConsumer(
    Tracing_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)
print( "--Tracing is Listening for tracing topic now--")
while True:

    for message in consumer:
        print("Trace notification")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        cust_id =  consumed_msg["customer_id"]
        state = consumed_msg["state"]
        
        print("--------------------------------------------------------------------------------------------------------------------")
#        print(f"INFO: Tracelevel-4  at Customer_id {cust_id}  is  {state}..!! ")
        logging.info(f"INFO: Tracelevel-4  at Customer_id {cust_id}  is  {state}..!! ")
#print( " testing2")