import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
nam = input("Enter name: ")
typ = input("Enter type of mortgage: ")
sal = input("Enter salary: ")
postcode = input("Enter postcode: ")

# convert into JSON:
#y = json.dumps(Customer)

# the result is a JSON string:
#print(y) 

DIP_Completed_TOPIC = "DIP_Details"

ORDER_KAFKA_TOPIC = "Order_Details"
ORDER_Limit = 4

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print( " to generate an order in 10 secs")

for i in range (1,ORDER_Limit):
    data = {
        "order_id" : i,
        "user_id" : f"{nam}_{i}",
        "salary"  : sal,
        "items" : f"{typ}",
        "postcode" : postcode
    }

    producer.send(
        ORDER_KAFKA_TOPIC,
        json.dumps(data).encode("utf-8")
    )
    print(f"done sending{i}")
    producer.flush()
    time.sleep(8)
"""""
consumer = KafkaConsumer(
    DIP_Completed_TOPIC,
    bootstrap_servers="localhost:9092"
)
print( " testing1")
for message in consumer:
        print("DIP notification")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        Inc_val =  consumed_msg["Income"]
        prop_val = consumed_msg["Property"]
        print(f"DIP result for consumer {nam} is income validation is {Inc_val} and property validation is {prop_val} ..!! ")
print( " testing2")"""