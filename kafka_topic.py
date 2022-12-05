#!/usr/bin/env python3

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from time import sleep
serverip = "127.0.0.1"
serverport = "9092"
kafkaserver = serverip + ":" + serverport
topicname = 'testtopic'
clientid = "windows"
messagecount = 10
topiclist = []

print("Connecting to KafkaAdminClient")
admin_client = KafkaAdminClient(
    bootstrap_servers=kafkaserver,
    client_id=clientid
    )

print("Printing list of Topics")
for topic in admin_client.list_topics():
    print(topic)

topiclist.append(NewTopic(
    name=topicname,
    num_partitions=1,
    replication_factor=1))

admin_client.create_topics(
    topiclist
    )

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

print("Deleting Topic")
admin_client.delete_topics(
    topics=[topicname])
