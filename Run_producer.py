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
#from requests_aws4auth import AWS4Auth
import json
import string
import random
import time
import classification.tweet_utils as tweet_utils

# change default system encoding method of python into UTF-8
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

from kafka import KafkaProducer
from kafka.errors import KafkaError

#Variables that contains the user credentials to access Twitter API
access_token = "***REMOVED***"
access_token_secret = "***REMOVED***"
consumer_key = "***REMOVED***"
consumer_secret = "***REMOVED***"

#This is a listener that process received tweets to stdout.
class TwitterListener(StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        try:
            tweet = json.loads(data) # load tweet content into json format

            if "place" in tweet.keys() and tweet["place"]!=None and tweet["lang"]=="en":
                # process coordinates information, format: [longitude, latitude]
                Box = tweet["place"]["bounding_box"]["coordinates"][0]
                XY = [(Box[0][0] + Box[2][0])/2, (Box[0][1] + Box[2][1])/2]
                # extract information into defined body format
                doc = {
                       "timestamp": tweet["timestamp_ms"],
                       "username": tweet["user"]["name"],
                       "created_at": tweet["created_at"].replace("+0000 ",""),
                       "geo_location": {'lat': XY[1], 'lon': XY[0]},
                       "place": tweet["place"]["full_name"],
                       "tweet_text": tweet["text"]
                       }
                tweet_utils._sentiment_analysis(doc)
                json_body = json.dumps(doc)
                # Send messages to Kafka Topic 'twitterstream'
                if doc["sentiment"] in ['Positive']:
                    #control throughput
                    if random.randint(1,30)==2:
                        self.producer.send('twitterstream_raw', json_body)
                        print json_body
                        print 'Write To Kafka Complete' + '\n'

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

def main():
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = TwitterListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.secure = True
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth=auth, listener=l, filter_level='medium', timeout=3000)

    while True:
        try:
            #This line filter Twitter Streams to capture data by the keywords
            #stream.filter(track=terms,languages=["en"])## This will feed the stream all mentions of 'keyword'
            #stream.sample()##This will feed the stream without keywords or filter
            #setting location to world wide
            stream.filter(locations=[-180,-90,180,90])
            break
        except Exception, e:
             # Abnormal exit: Reconnect
             print "Now sleep...Exception:", e
             nsecs=random.randint(10,20)
             time.sleep(nsecs)

if __name__ == '__main__':
    main()
