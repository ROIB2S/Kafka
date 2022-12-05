#!/usr/bin/env python3

from kafka import KafkaProducer
from time import sleep
serverip = "10.128.11.##" # replace with your IP
serverport = "9092"
kafkaserver = serverip + ":" + serverport
topicname = 'newtopic'
messagecount = 100

print("Connecting to Producer")
producer = KafkaProducer(
    bootstrap_servers=kafkaserver
    )
for counter in range(messagecount):
    output = "Test Message " + str(counter+1) + " of " + str(messagecount)
    print(output)
    producer.send(
        topicname, 
        bytes(output,'utf-8')
    )
    sleep(1)
