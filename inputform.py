import json
import kafka
import time
#import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket
import flask
from flask import Flask,jsonify,render_template,request

app = Flask(__name__)

@app.route("/")
def home():
    return "Under progress"

@app.route("/input", methods = ["POST","GET"])
def inputform():
    #nam = input("Enter name: ")
    #typ = input("Enter type of mortgage: ")
    #sal = input("Enter salary: ")
    #postcode = input("Enter postcode: ")
    reqjson = request.get_json()

    nam = reqjson['name']
    typ = reqjson['type']
    sal = reqjson['salary']
    postcode = reqjson['postcode']

    DIP_Completed_TOPIC = "DIP_Details"

    ORDER_KAFKA_TOPIC = "Order_Details"
    ORDER_Limit = 40

    producer = KafkaProducer(bootstrap_servers="192.168.1.112:9092")

    print( " to generate an customer details in 10 secs")

    for i in range (1,ORDER_Limit):
        data = {
            "Customer_id" : i,
            "user_id" : f"{nam}_{i}",
            "salary"  : sal,
            "items" : f"{typ}",
            "postcode" : postcode
        }
        time.sleep(2)
        producer.send(
            ORDER_KAFKA_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        print(f"done sending{i}")

        producer.flush()
    return "check dip status ms"
#        time.sleep(8)

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000)

