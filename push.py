import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def push(topic,tuple):
	global producer
	producer.send(topic, tuple)
	print 'pushed to', topic