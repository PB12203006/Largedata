#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Used to predict tweets sentiments

"""

import re
from textblob import TextBlob

class Sentiments:
    POSITIVE = 'Positive'
    NEGATIVE = 'Negative'
    NEUTRAL = 'Neutral'
    CONFUSED = 'Confused'

id_field = 'id_str'
emoticons = {Sentiments.POSITIVE:'😀|😁|😂|😃|😄|😅|😆|😇|😈|😉|😊|😋|😌|😍|😎|😏|😗|😘|😙|😚|😛|😜|😝|😸|😹|😺|😻|😼|😽',
             Sentiments.NEGATIVE : '😒|😓|😔|😖|😞|😟|😠|😡|😢|😣|😤|😥|😦|😧|😨|😩|😪|😫|😬|😭|😾|😿|😰|😱|🙀',
             Sentiments.NEUTRAL : '😐|😑|😳|😮|😯|😶|😴|😵|😲',
             Sentiments.CONFUSED: '😕'
             }

tweet_mapping = {'properties':
                    {'timestamp': {
                                  'type': 'date'
                                  },
                     'tweet_text': {
                                  'type': 'string'
                              },
                     'geo_location': {
                                'type': 'geo_point'
                                },
                     'username': {
                                'type': 'string'
                                },
                     'sentiment': {
                                  'type': 'string'
                              },
                     'created_at':{
                                  'type':'string'
                                  },
                     'place':{
                              'type':'string'
                              }
                    }
                 }

def _sentiment_analysis(tweet):
    #tweet['emoticons'] = []
    tweet['sentiment'] = []
    _sentiment_analysis_by_emoticons(tweet)
    if len(tweet['sentiment']) == 0:
        _sentiment_analysis_by_text(tweet)


def _sentiment_analysis_by_emoticons(tweet):
    for sentiment, emoticons_icons in emoticons.iteritems():
        matched_emoticons = re.findall(emoticons_icons, tweet['tweet_text'].encode('utf-8'))
        if len(matched_emoticons) > 0:
            #tweet['emoticons'].extend(matched_emoticons)
            tweet['sentiment'].append(sentiment)

    if Sentiments.POSITIVE in tweet['sentiment'] and Sentiments.NEGATIVE in tweet['sentiment']:
        tweet['sentiment'] = Sentiments.CONFUSED
    elif Sentiments.POSITIVE in tweet['sentiment']:
        tweet['sentiment'] = Sentiments.POSITIVE
    elif Sentiments.NEGATIVE in tweet['sentiment']:
        tweet['sentiment'] = Sentiments.NEGATIVE

def _sentiment_analysis_by_text(tweet):
    blob = TextBlob(tweet['tweet_text'].encode('utf-8').decode('ascii', errors="replace"))
    sentiment_polarity = blob.sentiment.polarity
    if sentiment_polarity < 0:
        sentiment = Sentiments.NEGATIVE
    elif sentiment_polarity <= 0.2:
                sentiment = Sentiments.NEUTRAL
    else:
        sentiment = Sentiments.POSITIVE
    tweet['sentiment'] = sentiment

def get_tweet(doc,count):
    tweet = doc
    tweet[id_field] = count
    #tweet['hashtags'] = map(lambda x: x['text'],doc['entities']['hashtags'])
    #tweet['coordinates'] = doc['coordinates']
    #tweet['timestamp_ms'] = doc['timestamp_ms']
    #tweet['text'] = doc['text']
    #tweet['user'] = {'id': doc['user']['id'], 'name': doc['user']['name']}
    #tweet['mentions'] = re.findall(r'@\w*', doc['text'])
    _sentiment_analysis(tweet)
    return tweet
