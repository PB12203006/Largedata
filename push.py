import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

def push(tuple):
	global producer
	producer.send('test', tuple)

