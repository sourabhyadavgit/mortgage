import json
import kafka
import time

from kafka import KafkaProducer
from kafka import KafkaConsumer

ORDER_KAFKA_TOPIC = "Order_Details"
ORDER_confirmed_TOPIC = "Order_confirmed"

producer = KafkaProducer(
#    ORDER_confirmed_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)

consumer = KafkaConsumer(
    ORDER_KAFKA_TOPIC,
    bootstrap_servers="192.168.1.112:9092"
)


print("--Income Validation starting to listen now--")
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
        print("----------------------------------------------------------------------------")
        producer.send(
            ORDER_confirmed_TOPIC,
            json.dumps(data).encode("utf-8")
        )
        producer.flush()