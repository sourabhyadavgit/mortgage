import json
import kafka
import time
import pymongo
from kafka import KafkaProducer
from kafka import KafkaConsumer
import bson
from bson.json_util import dumps
from bson import json_util

DIP_Completed_TOPIC = "DIP_Details"
ORDER_confirmed_TOPIC = "Order_confirmed"

producer = KafkaProducer(
#    DIP_Completed_TOPIC ,
    bootstrap_servers="localhost:9092")

consumer = KafkaConsumer(
    ORDER_confirmed_TOPIC,
    bootstrap_servers="localhost:9092"
)


print("starting to listen now")
while True:
    for message in consumer:
        print("Sending notification")
        consumed_msg = json.loads(message.value.decode("utf-8"))
        email = consumed_msg["email2"]
        status = consumed_msg["stat"]
        postcode = consumed_msg ["postcode"]
        
        my_client = pymongo.MongoClient("mongodb://rootsy:rootsy@localhost:27017")
        mydb = my_client["valh_data"]
        mycol = mydb["post_data"]
        document = mycol.find_one({"address":postcode})
        print("fetch value is: ")
        print(document)
        doc2 = json.loads(json_util.dumps(document))
        print(doc2)
        result = doc2['Status']

        data = {
            "Income" : status,
            "Property" : result,
        }

        producer.send(
            DIP_Completed_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        producer.flush()
        print(f"sent email to {email} and status is {status} and postcode is {postcode} and val hub result is {result}")


