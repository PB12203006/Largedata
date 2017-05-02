"""

/PATH-TO-SPARK/bin/spark-submit tfidf_classify_spark.py

Comparing performance of binary classification models using restaurant reviews data, labeled by {positive(+1), negative(-1)}
NaiveBayes; SVMWithSGD; LogisticRegressionWithLBFGS; PerceptronforRDD with various parameters

by Yilan Ji 

"""

from pyspark import SparkContext
from pyspark.sql import Row
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, SVMWithSGD, SVMModel, LogisticRegressionWithLBFGS, LogisticRegressionModel
from perceptron import PerceptronforRDD

import numpy as np

sc = SparkContext()
sc.setLogLevel("ERROR")
# Load documents (one per line).
#documents = sc.textFile("tweets_test.txt").map(lambda line: line.split(" "))

reviewdata5 = sc.textFile('data/reviews_tr5000.txt')
reviewdata1 = sc.textFile('data/reviews_tr1000.txt')
labels = sc.textFile('data/reviews_tr5000_label.txt')
#print(labels.collect())
testlabels = sc.textFile('data/reviews_tr1000_label.txt')
numfeatures=2000
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
tf = HashingTF(numFeatures=numfeatures).transform( ## Use much larger number in practice
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

testtf = HashingTF(numFeatures=numfeatures).transform( ## Use much larger number in practice
    test_raw.map(lambda doc: doc["text"].split(),
    preservesPartitioning=True))

testtf.cache()
testidf = IDF().fit(testtf)
testtfidf = testidf.transform(testtf)

# Combine using zip
test = testlabels.zip(testtf).map(lambda x: LabeledPoint(x[0], x[1]))
errors = []
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

#print 'Confusion Matrix of NaiveBayes Model:'
m = metrics.confusionMatrix().toArray()
#print metrics.confusionMatrix().toArray()
errrate = float(m[0][1]+m[1][0])/float(m[0][1]+m[1][0]+m[0][0]+m[1][1])
errors.append(Row(truePositive=int(m[1][1]), trueNegative=int(m[0][0]), falseNegative=int(m[0][1]), \
                falsePositive=int(m[1][0]), errorRate=float(errrate), Model="NaiveBayes"))
print "Test error rate of NaiveBayes Model: ", errrate

"""
Use SVMWithSGD model from pyspark.mllib
"""

model = SVMWithSGD.train(training, iterations=100)

labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

#print 'Confusion Matrix of SVMWithSGD Model:'
m = metrics.confusionMatrix().toArray()
#print metrics.confusionMatrix().toArray()
errrate = float(m[0][1]+m[1][0])/float(m[0][1]+m[1][0]+m[0][0]+m[1][1])
errors.append(Row(truePositive=int(m[1][1]), trueNegative=int(m[0][0]), falseNegative=int(m[0][1]), \
                falsePositive=int(m[1][0]), errorRate=float(errrate), Model="SVMWithSGDItr100"))
print "Test error rate of SVMWithSGD Model(iterations=100): ", errrate

"""
Use LogisticRegressionWithLBFGS model from pyspark.mllib
"""

model = LogisticRegressionWithLBFGS.train(training)
labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

#print 'Confusion Matrix of LogisticRegressionWithLBFGS Model:'
m = metrics.confusionMatrix().toArray()
#print metrics.confusionMatrix().toArray()
errrate = float(m[0][1]+m[1][0])/float(m[0][1]+m[1][0]+m[0][0]+m[1][1])
errors.append(Row(truePositive=int(m[1][1]), trueNegative=int(m[0][0]), falseNegative=int(m[0][1]), \
                falsePositive=int(m[1][0]), errorRate=float(errrate), Model="LogisticRegressionWithLBFGS"))
print "Test error rate of LogisticRegressionWithLBFGS Model: ", errrate

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
#print 'example testlabels for perceptron:',testlabel.take(5)

modelOP = PerceptronforRDD(numfeatures)
[w,b] = modelOP.PerceptronBatch(tf, label,MaxItr=1)
predict = modelOP.Predict(testtf)
#print 'example predict labels for single-pass perceptron:',predict.take(5)
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errrate = modelOP.PredictErrrate(testtf,testlabel)
print 'Test error rate of OnlinePerceptron(iterations=1): ',errrate
#print "Confusion Matrix of OnlinePerceptron Model : "
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="OnlinePerceptronItr10"))
#print 'Test error rate using single pass perceptron: ',errrate

modelOP10 = PerceptronforRDD(numfeatures)
[w,b] = modelOP10.PerceptronBatch(tf, label,MaxItr=1)
predict = modelOP10.Predict(testtf)
#print 'example predict labels for single-pass perceptron:',predict.take(5)
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errrate = modelOP.PredictErrrate(testtf,testlabel)
print 'Test error rate of OnlinePerceptron(iterations=10): ',errrate
#print "Confusion Matrix of OnlinePerceptron Model : "
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="OnlinePerceptronItr10"))
#print 'Test error rate using single pass perceptron: ',errrate

model = PerceptronforRDD(numfeatures)
[w,b] = model.AveragePerceptron(tf, label,MaxItr=1)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=1): ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronItr1"))


#model = PerceptronforRDD(numfeatures)
model = modelOP
[w,b] = model.AveragePerceptron(tf, label,MaxItr=1)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=1) after single-pass OnlinePerceptron: ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronOPItr1"))

#model = PerceptronforRDD(numfeatures)
model = modelOP10
[w,b] = model.AveragePerceptron(tf, label,MaxItr=10)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=10) after 10 time pass OnlinePerceptron: ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronOPItr10"))

model = PerceptronforRDD(numfeatures)
[w,b] = model.AveragePerceptron(tf, label,MaxItr=5)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=5):: ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronItr5"))

model = PerceptronforRDD(numfeatures)
[w,b] = model.AveragePerceptron(tf, label,MaxItr=10)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=10): ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronItr10"))

model = PerceptronforRDD(numfeatures)
[w,b] = model.AveragePerceptron(tf, label,MaxItr=100)
predict = model.Predict(testtf)
#print 'example predict labels for average perceptron:',predict.take(5)
errrate = model.PredictErrrate(testtf,testlabel)
print 'Test error rate of AveragePerceptron(iterations=100): ',errrate
#print "Confusion Matrix of AveragePerceptron Model : "
truePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==1)+0).sum()
trueNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]==x[1] and x[0]==-1)+0).sum()
falseNegative = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==1)+0).sum()
falsePositive = testlabel.zip(predict).map(lambda x: 1*(x[0]!=x[1] and x[0]==-1)+0).sum()
errors.append(Row(truePositive=int(truePositive), trueNegative=int(trueNegative), falseNegative=int(falseNegative), \
                falsePositive=int(falsePositive), errorRate=float(errrate), Model="AveragePerceptronItr100"))

print "\n"
print "Confusion Matrix of models:"
sc.parallelize(errors).toDF().show(truncate=False)

"""
Classify a batch of tf/tfidf after collcet() using Online Perceptron and Avarage Perceptron
"""
"""
from scipy.sparse import coo_matrix

# m sparse matrix
# d dimension
# y label
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
"""
