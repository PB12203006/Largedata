"""
/PATH-TO-SPARK/bin/spark-submit category_news_train_nb.py

This script is to train a NaiveBayes Multiclass Model using news collected from New York Times in 2016 as training data.
This news trained model only used for categorizing tweets regardless of correct feedback.
by Yilan Ji

"""
from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession, Row
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from pyspark.ml.classification import NaiveBayes,NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

sc = SparkContext()
spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
raw_data = sc.textFile("data/news_sections_abstract2016.txt")
lines = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))
sentenceData = spark.createDataFrame(lines,["label", "sentence"])

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
featurizedData = hashingTF.transform(wordsData)
#featurizedData.show()

df = featurizedData.select('label','features')
data0 = df.replace(['World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy','Art & Design'] \
            ,['1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16','17','18','0'],'label')

category = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']
dictionary = {'Art & Design':0,'World':1,'Sports':2,'Fashion & Style':3,'Books':4,'Music':5, \
            'Television':6,'Movies':7,'Technology':8,'Science':9,'Food':10,'Real Estate':11,'Theater':12, \
            'Health':13,'Travel':14,'Education':15,'Your Money':16,'Politics':17,'Economy':18}
labeleddata = data0.select(data0.label.cast("double").alias('label'),'features').na.drop()
labeleddata.cache()
(train, test) = labeleddata.randomSplit([0.8, 0.2])

"""
NaiveBayes
"""


# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

model = nb.fit(train)
#path = tempfile.mkdtemp()
model.save("models/NaiveBayes_model/")
#sameModel = NaiveBayesModel.load("/Users/Jillian/Documents/Python/large_data_pj/NaiveBayes_model/")

# select example rows to display.
tt = test.select("features").rdd.map(lambda x: x.features).collect()
"""
Here, replace tt in tt.map() as testtf
"""
tt = tt.map(lambda x: Row(features=x)).toDF()
tt.show()
predictions = model.transform(tt)
#predictions.show()

labels = predictTweetCategNB(tt,sc)
print labels[:10]

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
