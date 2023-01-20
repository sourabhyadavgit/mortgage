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
    bootstrap_servers="192.168.1.112:9092")

consumer = KafkaConsumer(
    ORDER_confirmed_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)


print("--Property Check MS starting to listen now--")
while True:
    for message in consumer:
        print("------------Income Validation step for the Customer------------")

        consumed_msg = json.loads(message.value.decode("utf-8"))
        email = consumed_msg["email2"]
        status = consumed_msg["stat"]
        postcode = consumed_msg ["postcode"]
        userid = consumed_msg["user"]
        
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
            "userid": userid
        }
        data2 = {
            "customer_id" : userid,
            "state" : "at Property validation microservice "
        }
        Tracing_TOPIC = "trace_details"
        producer.send(
            DIP_Completed_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        producer.send(
            Tracing_TOPIC,
            json.dumps(data2).encode("utf-8")
        )
        producer.flush()
        print(f"sent email to {email} and property status for {postcode} is {result} via val_hub")
        print("----------------------------------------------------------------------------")

