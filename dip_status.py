import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer

DIP_Completed_TOPIC = "DIP_Details"
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
        print("--------------------------------------------------------------------------------------------------------------------")
        print(f"DIP-1 result for consumerid {userid}  is income validation is {Inc_val} and property validation is {prop_val} ..!! ")
#print( " testing2")