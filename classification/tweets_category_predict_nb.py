from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession, Row
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from pyspark.ml.classification import NaiveBayes,NaiveBayesModel
category = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']
def predictTweetCategNB(testtf,sc):
    modelTweetCategoryNB = NaiveBayesModel.load("/home/pb12203006/Documents/Largedata_with_Princess/classification/NaiveBayes_model/")
    # select example rows to display.
    tt = sc.parallelize(testtf).map(lambda x: Row(features=x)).toDF()
    tt.show()
    predictions = modelTweetCategoryNB.transform(tt)
    #predictions.show()
    labels = predictions.select("prediction").rdd.map(lambda x: category[int(x.prediction)]).collect()
    return labels