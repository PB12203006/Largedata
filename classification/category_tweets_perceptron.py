"""
/PATH-TO-SPARK/bin/spark-submit category_tweets_perceptron.py

Train 4 multiclass perceptron models with different parameters online
Training data is feedback on tweets corrct categories.
After trained about 80 tweets, models predict same 20 tweets and print the predictions.
Comparing the predictions with true labels

by Yilan Ji

"""

from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession, Row
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from perceptron import PerceptronforRDD
from tweets_category_predict_perceptron import predictTweetCategPerceptron
from perceptronMulticlass import MulticlassPerceptron
sc = SparkContext()
spark = SparkSession.builder \
        .master("local") \
        .appName("Perceptron train News Category") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
raw_data = sc.textFile("data/tweetsCorrectIndex.txt")

lines = raw_data.map(lambda line: line.split("    ")).map(lambda line: (line[0]," ".join(line[1:])))
sentenceData = spark.createDataFrame(lines,["label", "sentence"])

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
featurizedData = hashingTF.transform(wordsData)
#featurizedData.show()
#idf = IDF(inputCol="rawFeatures", outputCol="features")
#idfModel = idf.fit(featurizedDataTF)
#featurizedData = idfModel.transform(featurizedDataTF)

df = featurizedData.select('label','features','sentence')
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
labeleddata = data0.select(data0.label.cast("double").alias('label'),'features','sentence').na.drop()
labeleddata.cache()
(train, test) = labeleddata.randomSplit([0.8, 0.2])

"""
Multiclass Perceptron
"""

#dataset = train
#traindata = dataset.select('features').rdd.map(lambda row: row.features)
#trainlabels = dataset.select('label').rdd.map(lambda row: row.label)

testdataset = labeleddata
testdata = testdataset.select('features').rdd.map(lambda row: row.features)
testlabels = testdataset.select('label').rdd.map(lambda row: row.label)
testsentence = testdataset.select('sentence').rdd.map(lambda row: row.sentence)


testlabels = testlabels.map(lambda x:("Others")*(x==-1)+(x!=-1)*category[int(x)])
#print testlabels.collect()
tweetsLabels = testlabels.zip(testsentence).map(lambda x: Row(category=x[0],text=x[1])).toDF()
tweetsLabels.show(truncate=False)
#print testlabels.map(lambda x:("Others")*(x==-1)+(x!=-1)*category[int(x)]).collect()
initialModelOP = MulticlassPerceptron(dictionary=dictionary,category=category)
predsInit = sc.parallelize(initialModelOP.predict(testdata)).map(lambda x: " ".join(x))
#print predsInit.collect()
initialModelAP = MulticlassPerceptron(dictionary=dictionary,category=category)
initialModelAP.load("models/perceptronModels.json",average=False)
predsInit = predsInit.zip(sc.parallelize(initialModelAP.predict(testdata))).map(lambda x: [x[0]," ".join(x[1][:])])
#print predsInit.collect()
loadModelOP = MulticlassPerceptron(dictionary=dictionary,category=category)
loadModelOP.load("models/perceptronModels.json",average=False)
predsLoad = sc.parallelize(loadModelOP.predict(testdata)).map(lambda x: " ".join(x))
#print predsLoad.collect()
#loadModelOP = MulticlassPerceptron(dictionary=dictionary,category=category)
#loadModelOP.load("/Users/Jillian/Documents/Python/large_data_pj/perceptronModels.json",average=False)
loadModelAP = MulticlassPerceptron(dictionary=dictionary,category=category)
loadModelAP.load("models/perceptronModels.json",average=False)
predsLoad = predsLoad.zip(sc.parallelize(loadModelAP.predict(testdata))).map(lambda x: [x[0]," ".join(x[1][:])])

#print predsLoad.collect()
preds = predsInit.zip(predsLoad).map(lambda x:x[0]+x[1])
#print preds.collect()
compareLabels = preds#.zip(testlabels)
compareLabels = compareLabels.map(lambda x: Row(OnlinePerceptron=x[0],NewsPerceptronOPItr10=x[1],\
                                    NewsPerceptronAPItr1=x[2],NewsPerceptronAPItr10=x[3])).toDF()
compareLabels.show(truncate=False)

(dataset1,dataset2) = train.randomSplit([0.5, 0.5])
(dataset01,dataset02) = dataset1.randomSplit([0.5,0.5])
(dataset03,dataset04) = dataset2.randomSplit([0.5,0.5])
traindataset = [dataset01,dataset02,dataset03,dataset04]
dataset = labeleddata
traindata = dataset.select('features').rdd.map(lambda row: row.features)
trainlabels = dataset.select('label').rdd.map(lambda row: row.label).map(lambda x: ("Others")*(x==-1)+(x!=-1)*category[int(x)])
#initialModelAP.train(traindata,trainlabels,method="Online")
for i in range(4):
    dataset = traindataset[i]
    traindata = dataset.select('features').rdd.map(lambda row: row.features)
    trainlabels = dataset.select('label').rdd.map(lambda row: row.label).map(lambda x: ("Others")*(x==-1)+(x!=-1)*category[int(x)])

    print "Training 4 MulticlassPerceptron models using",dataset.count(),"training data.............................\n"
    # train an Online Perceptron model(Iteration=10) use part of training data, initialize model with all zeros
    initialModelOP.train(traindata,trainlabels,method="Online",MaxItr=10)
    # train an Online Perceptron model(Iteration=10) use part of training data, intialize model with news trained model(ignore history)
    initialModelAP.train(traindata,trainlabels,method="Online",MaxItr=10)
    # train an Average perceptron model(Iteration=1) use part of training data, initialize using News trained Model(ignore history)
    loadModelOP.train(traindata,trainlabels,method="Average",MaxItr=1)
    # train an Average perceptron model(Iteration=10) use part of training data, initialize using News trained Model(ignore history)
    loadModelAP.train(traindata,trainlabels,method="Average",MaxItr=10)

    print "Predicting labels of test Tweets after trained ",dataset.count()," Category Feedback Tweets:\n"

    predsInit = sc.parallelize(initialModelOP.predict(testdata)).map(lambda x: " ".join(x))
    predsInit = predsInit.zip(sc.parallelize(initialModelAP.predict(testdata))).map(lambda x: [x[0]," ".join(x[1][:])])
    #print predsInit.count()

    predsLoad = sc.parallelize(loadModelOP.predict(testdata)).map(lambda x: " ".join(x))
    predsLoad = predsLoad.zip(sc.parallelize(loadModelAP.predict(testdata))).map(lambda x: [x[0]," ".join(x[1][:])])
    preds = predsInit.zip(predsLoad).map(lambda x:x[0]+x[1])
    compareLabels = preds#.zip(testlabels)
    compareLabels = compareLabels.map(lambda x: Row(OnlinePerceptron=x[0],NewsPerceptronOPItr10=x[1],\
                            NewsPerceptronAPItr1=x[2],NewsPerceptronAPItr10=x[3])).toDF()
    compareLabels.show(truncate=False)
