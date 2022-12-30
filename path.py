import json
import kafka
import time
import pymongo
from kafka import KafkaConsumer
from kafka import KafkaProducer
import socket
import flask
from flask import Flask,jsonify,render_template,request

app = Flask(__name__)

@app.route("/")
def home():
    return "Set progress"

@app.route("/foo", methods = ["POST"])
def inputform():
    data = request.json
    x = data["name"]
    y = data["age"]
    return "test {x} and {y}" + x + y
    #time.sleep(8)

print("hello SMY")

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)

