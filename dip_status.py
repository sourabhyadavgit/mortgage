import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
import pymongo
import bson
from bson.json_util import dumps
from bson import json_util

DIP_Completed_TOPIC = "DIP_Details"
Tracing_TOPIC = "trace_details"

producer = KafkaProducer(
#    ORDER_confirmed_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)
consumer = KafkaConsumer(
    DIP_Completed_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)
print( "--DIP Status Listening for Connections now--")
while True:

    for message in consumer:
        print("DIP notification")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        Inc_val =  consumed_msg["Income"]
        prop_val = consumed_msg["Property"]
        userid = consumed_msg["userid"]
        data2 = {
            "customer_id" : userid,
            "state" : "at DIP status microservice "
        }
        producer.send(
            Tracing_TOPIC,
            json.dumps(data2).encode("utf-8")
        )
        print("--------------------------------------------------------------------------------------------------------------------")
        print(f"DIP-1 result for consumerid {userid}  is income validation is {Inc_val} and property validation is {prop_val} ..!! ")
#print( " testing2")