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

"""
Use NaiveBayes model from pyspark.mllib
"""

# Train and check
model = NaiveBayes.train(training)
labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])
#print(errrate)
from pyspark.mllib.evaluation import MulticlassMetrics
from operator import itemgetter

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

print(metrics.confusionMatrix().toArray())

"""
Use SVMWithSGD model from pyspark.mllib
"""

model = SVMWithSGD.train(training, iterations=100)

labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

print(metrics.confusionMatrix().toArray())

"""
Use LogisticRegressionWithLBFGS model from pyspark.mllib
"""

model = LogisticRegressionWithLBFGS.train(training)
labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

print(metrics.confusionMatrix().toArray())



#print([[tf[i], label[i]] for i in range(label.count())])
#traindata = tf.zip(label).map(lambda p: Row(review=p[0], label=p[1]))
#training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))

#testdata = tf_test.zip(testlabel).map(lambda p: Row(review=p[0], label=p[1]))
# spark.mllib's IDF implementation provides an option for ignoring terms
# which occur in less than a minimum number of documents.
# In such cases, the IDF for these terms is set to 0.
# This feature can be used by passing the minDocFreq value to the IDF constructor.
#idfIgnore = IDF(minDocFreq=2).fit(tf)
#tfidfIgnore = idfIgnore.transform(tf)
# Load and parse the data
#def parsePoint(line):
#    values = [float(x) for x in line.split(' ')]
#    return LabeledPoint(values[0], values[1:])

#data = sc.textFile("data/mllib/sample_svm_data.txt")
#parsedData = data.map(parsePoint)
"""
Classify a batch of tf/idf using Perceptron based on RDD
"""

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


"""
Classify a batch of tf/tfidf after collcet() using Online Perceptron and Avarage Perceptron
"""

from scipy.sparse import coo_matrix

# m sparse matrix
# d dimension
# y label
# m should be preprocessed into a sparse matrix,so currently this function may not work
def Perceptron_train(m,y):
	w=np.zeros(m[1].size)
	b=0
	y = [-1*(x==0 or x==-1)+(x==1) for x in y]
	for i in range (len(m)):
		a=m[i].dot(w)+b
		if y[i]*a<=0:
			w=w+y[i]*m[i].toArray()
			b=b+y[i]
	return [w,b]


def AveragePerceptron(data, label):
	w = np.zeros(data[1].size)
	u = np.zeros(data[1].size)
	b = 0
	c = 1
	beta = 0
	label = [-1*(x==0 or x==-1)+(x==1) for x in label]
	for i in range(len(data)):
		predict = data[i].dot(w) + b
		if label[i]*predict<0 or label[i]*predict==0:
			w = w + label[i]*data[i].toArray()
			b = b + label[i]
			u = u + c*label[i]*data[i].toArray()
			beta = beta + c*label[i]
		c += 1
	w = w - u/c
	b = b - beta/c
	return [w,b]

def PerceptronPredict(testdata,w,b):
	predict = []
	for i in range(len(testdata)):
		p = testdata[i].dot(w) + b
		p = -1*(p<0)+1*(p>=0)
		predict.append(p)
	return predict

tfidf = tfidf.collect()
testtfidf = testtfidf.collect()
label = label.collect()
testlabel = testlabel.collect()
[w,b] = Perceptron_train(tfidf, label)
predict = PerceptronPredict(testtfidf, w, b)
testlabel = [-1*(x==0 or x==-1)+(x==1) for x in testlabel]
ones = [1*(testlabel[i]!=predict[i]) for i in range(len(testlabel))]
err = sum(ones)
print(err)
errrate = float(err)/float(len(testlabel))
print(errrate)
[w,b] = AveragePerceptron(tfidf, label)
predict = PerceptronPredict(testtfidf, w, b)
testlabel = [-1*(x==0 or x==-1)+(x==1) for x in testlabel]
ones = [1*(testlabel[i]!=predict[i]) for i in range(len(testlabel))]
err = sum(ones)
print(err)
errrate = float(err)/float(len(testlabel))
print(errrate)
