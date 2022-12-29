import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer

DIP_Completed_TOPIC = "DIP_Details"
consumer = KafkaConsumer(
    DIP_Completed_TOPIC,
    bootstrap_servers="localhost:9092"
)
print( " testing1")
while True:

    for message in consumer:
        print("DIP notification")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        Inc_val =  consumed_msg["Income"]
        prop_val = consumed_msg["Property"]
        print(f"DIP result for consumer  is income validation is {Inc_val} and property validation is {prop_val} ..!! ")
#print( " testing2")