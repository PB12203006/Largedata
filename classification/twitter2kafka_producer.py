#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
@author: Jillian

Upload filtered tweets from Twitter API to Kafka by KafkaProducer()
classify based on text and sentiment(as label)
train a perceptron model
Send Notification if predict label is 1.0

"""
#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from requests_aws4auth import AWS4Auth
import json
import string
import random
import time
import tweet_utils
import boto3
from perceptron import PerceptronforRDD
from pyspark.mllib.feature import HashingTF, IDF
import pyspark

# change default system encoding method of python into UTF-8
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

from kafka import KafkaProducer
from kafka.errors import KafkaError
#producer = KafkaProducer(bootstrap_servers=['localhost:9092'])#, value_serializer=lambda v: json.dumps(v).encode('ascii'))


sc=pyspark.SparkContext(appName="twittertest")
sc.setLogLevel("WARN")
model = PerceptronforRDD(200)

#Variables that contains the user credentials to access Twitter API
access_token = "***REMOVED***"
access_token_secret = "***REMOVED***"
consumer_key = "***REMOVED***"
consumer_secret = "***REMOVED***"


#This is a listener that process received tweets to stdout.
class TwitterListener(StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        self.client_sns = boto3.client('sns')
        self.sns_arn = 'arn:aws:sns:us-west-2:560376101737:tweets-feed'

    def on_data(self, data):
        try:
            tweet = json.loads(data) # load tweet content into json format

            if "place" in tweet.keys() and tweet["place"]!=None and tweet["lang"]=="en":
                # process coordinates information, format: [longitude, latitude]
                Box = tweet["place"]["bounding_box"]["coordinates"][0]
                XY = [(Box[0][0] + Box[2][0])/2, (Box[0][1] + Box[2][1])/2]
                # extract information into defined body format
                #print tweet["text"]
                doc = {
                       "timestamp": tweet["timestamp_ms"],
                       "username": tweet["user"]["name"],
                       "created_at": tweet["created_at"].replace("+0000 ",""),
                       "geo_location": {'lat': XY[1], 'lon': XY[0]},
                       "place": tweet["place"]["full_name"],
                       #"hashtags": tweet["entities"]["hashtags"],
                       "tweet_text": tweet["text"]
                       }
                tweet_utils._sentiment_analysis(doc)
                data = {"text":doc["tweet_text"], "label":doc["sentiment"]}
                if data["label"]=='Negative' or data["label"]=='Neutral':
                    data["label"]=-1.0
                elif data["label"]=='Positive':
                    data["label"]=1.0
                print 'Training data:\n',data
                #print '\n'
                [tf, labels] = Tf([data])
                print 'Training data after TF:\n',[tf.first(),labels.first()]
                #print '\n'
                [model,w,b] = ClassifybyPerceptron(tf,labels)
                print 'Perceptron weight:\n',[w,b]
                #print '\n'
                predict_label = model.Predict(tf)
                print 'Predict sentiment:\n',predict_label.first()
                #print '\n'
                #errrate = model.PredictErrrate(tf,labels)
                #print 'Training error rate:', errrate
                json_body = json.dumps(doc)
                print json_body
                # Send messages to Kafka Topic 'twitterstream'
                if predict_label.first()==1.0:
                    self.producer.send('twitterstream', json_body)
                    print 'Write To Kafka Complete' + '\n'
                    self.client_sns.publish(TopicArn=self.sns_arn, Message=json_body, Subject='Tweet')
                    print 'message pushed'
                print '\n\n\n'


        except Exception, e:
            print e
            pass
        return True


    def on_status(self, status):
        try:
            print status
        except Exception, e:
            print e
            pass


    def on_error(self, status):
        try:
            print status
        except Exception, e:
            print e
            pass

    def on_connect(self):
        print (self)

def Tfidf(data):
	training_raw = sc.parallelize(data)
	labels = training_raw.map(
    	lambda doc: doc["label"],  # Standard Python dict access
    	preservesPartitioning=True # This is obsolete.
	)
	# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
	# First to compute the IDF vector and second to scale the term frequencies by IDF.
	tf = HashingTF(numFeatures=200).transform( ## Use much larger number in practice
    	training_raw.map(lambda doc: doc["text"].split(),
    	preservesPartitioning=True))
	#tf.cache()
	idf = IDF().fit(tf)
	tfidf = idf.transform(tf)
	return tfidf

def Tf(data):
	training_raw = sc.parallelize(data)
	labels = training_raw.map(
    	lambda doc: doc["label"],  # Standard Python dict access
    	preservesPartitioning=True # This is obsolete.
	)
	# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
	# First to compute the IDF vector and second to scale the term frequencies by IDF.
	tf = HashingTF(numFeatures=200).transform( ## Use much larger number in practice
    	training_raw.map(lambda doc: doc["text"].split(),
    	preservesPartitioning=True))
	return [tf,labels]

def ClassifybyPerceptron(tf,label):
	[w,b] = model.AveragePerceptron(tf, label)
	return [model,w,b]

def main():
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = TwitterListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth=auth, listener=l, timeout=3000)
    #This list a preselected keywords
    terms = ['Trump', 'Putin','China','bagel','pizza','salmon','coffee','dating','travel','java','python','ruby']

    while True:
        try:
            #This line filter Twitter Streams to capture data by the keywords
            stream.filter(track=terms,filter_level='low',languages=["en"])## This will feed the stream all mentions of 'keyword'
            #stream.sample()##This will feed the stream without keywords or filter
            break
        except Exception, e:
             # Abnormal exit: Reconnect
             print "Now sleep...Exception:", e
             nsecs=random.randint(10,20)
             time.sleep(nsecs)


if __name__ == '__main__':
    main()
