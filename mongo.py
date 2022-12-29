import json
from json import dumps
from kafka import KafkaProducer
from time import sleep
import pymongo
import bson
from bson.json_util import dumps
from bson import json_util

my_client = pymongo.MongoClient("mongodb://rootsy:rootsy@localhost:27017")

mydb = my_client["valh_data"]
mycol = mydb["post_data"]

mydict = {"address": "wa15", "Status": "Bad"}

mycol.insert_one(mydict)
print("submitted a record into mongodb")
