from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, SVMWithSGD, SVMModel, LogisticRegressionWithLBFGS, LogisticRegressionModel
from perceptron import PerceptronforRDD

import numpy as np

sc = SparkContext()
# Load documents (one per line).
#documents = sc.textFile("tweets_test.txt").map(lambda line: line.split(" "))

reviewdata5 = sc.textFile('data/reviews_tr5000.txt')
reviewdata1 = sc.textFile('data/reviews_tr1000.txt')
labels = sc.textFile('data/reviews_tr5000_label.txt')
#print(labels.collect())
testlabels = sc.textFile('data/reviews_tr1000_label.txt')

# TRAIN DATA
label = labels.map(lambda line: float(line))
t = reviewdata5.collect()
l = label.collect()
traindata = [{"text":t[i],"label":l[i]} for i in range(len(l))]

training_raw = sc.parallelize(traindata)


labels = training_raw.map(
    lambda doc: doc["label"],  # Standard Python dict access
    preservesPartitioning=True # This is obsolete.
)


# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
# First to compute the IDF vector and second to scale the term frequencies by IDF.
tf = HashingTF(numFeatures=200).transform( ## Use much larger number in practice
    training_raw.map(lambda doc: doc["text"].split(),
    preservesPartitioning=True))

tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

# Combine using zip
training = labels.zip(tf).map(lambda x: LabeledPoint(x[0], x[1]))

# TEST DATA
testlabel = testlabels.map(lambda line: float(line))
t = reviewdata1.collect()
l = testlabel.collect()
testdata = [{"text":t[i],"label":l[i]} for i in range(len(l))]

test_raw = sc.parallelize(testdata)

testlabels = test_raw.map(
    lambda doc: doc["label"],  # Standard Python dict access
    preservesPartitioning=True # This is obsolete.
)

testtf = HashingTF(numFeatures=200).transform( ## Use much larger number in practice
    test_raw.map(lambda doc: doc["text"].split(),
    preservesPartitioning=True))

testtf.cache()
testidf = IDF().fit(testtf)
testtfidf = testidf.transform(testtf)

# Combine using zip
test = testlabels.zip(testtf).map(lambda x: LabeledPoint(x[0], x[1]))


label = labels.map(lambda line: float(line))
testlabel = testlabels.map(lambda line: float(line))
testlabel = testlabel.map(lambda x: -1.0*(x==0 or x==-1)+1.0*(x==1))
print(testlabel.take(5))

model = PerceptronforRDD(200)
[w,b] = model.PerceptronBatch(tf, label)
predict = model.Predict(testtf)
print(predict.take(5))
errrate = model.PredictErrrate(testtf,testlabel)
print(errrate)

model = PerceptronforRDD(200)
[w,b] = model.AveragePerceptron(tf, label)
predict = model.Predict(testtf)
print(predict.take(5))
errrate = model.PredictErrrate(testtf,testlabel)
print(errrate)
