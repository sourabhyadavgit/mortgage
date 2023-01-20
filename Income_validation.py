import json
import kafka
import time
import pymongo
import bson
from bson.json_util import dumps
from bson import json_util


from kafka import KafkaProducer
from kafka import KafkaConsumer

ORDER_KAFKA_TOPIC = "Order_Details"
ORDER_confirmed_TOPIC = "Order_confirmed"
Tracing_TOPIC = "trace_details"

producer = KafkaProducer(
#    ORDER_confirmed_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)


print("--Income Validation starting to listen input-topic now--")
while True:
    for message in consumer:
        print("------------Income Validation step for the Customer------------")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        print(consumed_msg)

        userid = consumed_msg["Customer_id"]
        email = consumed_msg["user_id"]
        postcode = consumed_msg["postcode"]
        sal = int(consumed_msg["salary"])
        if sal > 20 :
            status = "Good"
        else:
            status = "Bad"

        data = {
            "user" : userid,
            "email2" : f"{email}@gmail.com",
            "stat" : status,
            "postcode" : postcode
        }
        data2 = {
            "customer_id" : userid,
            "state" : "at income validation microservice "
        }
        my_client = pymongo.MongoClient("mongodb://rootsy:rootsy@localhost:27017")

        mydb = my_client["Income_data"]
        mycol = mydb["Income_data"]

        mydict = {"address": userid, "Status": status,"postcode": postcode}

        mycol.insert_one(mydict)
        print("submitted a record into mongodb")
        print("----------------------------------------------------------------------------")
        producer.send(
            ORDER_confirmed_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        producer.send(
            Tracing_TOPIC,
            json.dumps(data2).encode("utf-8")
        )
        producer.flush()