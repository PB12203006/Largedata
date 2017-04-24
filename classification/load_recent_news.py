from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

sc = SparkContext()

raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/news_recent_headline.txt")
headline = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))

raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/news_recent_abstract.txt")
abstract = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))

raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/news_recent_url.txt")
url = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))

new = headline.zip(abstract).map(lambda (x,y):(x[0],(x[1],y[1])))
news = new.zip(url).map(lambda (x,y):{"label":x[0],"content":{"headline":x[1][0],"abstract":x[1][1],"url":y[1]}})

"""
news:
{
    label: String,
    content: {
        "headline": String,
        "abstract": String,
        "url": String
    }
}

"""
