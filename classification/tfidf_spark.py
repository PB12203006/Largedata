from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, SVMWithSGD, SVMModel, LogisticRegressionWithLBFGS, LogisticRegressionModel

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

tf = HashingTF(numFeatures=100).transform( ## Use much larger number in practice
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

testtf = HashingTF(numFeatures=100).transform( ## Use much larger number in practice
    test_raw.map(lambda doc: doc["text"].split(),
    preservesPartitioning=True))

testtf.cache()
testidf = IDF().fit(testtf)
testtfidf = testidf.transform(testtf)

# Combine using zip
test = testlabels.zip(testtf).map(lambda x: LabeledPoint(x[0], x[1]))

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


model = SVMWithSGD.train(training, iterations=100)

labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

print(metrics.confusionMatrix().toArray())



model = LogisticRegressionWithLBFGS.train(training)
labels_and_preds = testlabels.zip(model.predict(testtf)).map(
    lambda x: {"actual": x[0], "predicted": float(x[1])})
#print(labels_and_preds.collect()[:20])

metrics = MulticlassMetrics(
    labels_and_preds.map(itemgetter("actual", "predicted")))

print(metrics.confusionMatrix().toArray())

# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
# First to compute the IDF vector and second to scale the term frequencies by IDF.
#tf.cache()
#tf_test.cache()
#idf = IDF().fit(tf)
#idf_test = IDF().fit(tf_test)
#tfidf = idf.transform(tf)
#tfidf_test = idf_test.transform(tf_test)


#print([[tf[i], label[i]] for i in range(label.count())])
#traindata = tf.zip(label).map(lambda p: Row(review=p[0], label=p[1]))
#training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
#print(training)
#testdata = tf_test.zip(testlabel).map(lambda p: Row(review=p[0], label=p[1]))
# spark.mllib's IDF implementation provides an option for ignoring terms
# which occur in less than a minimum number of documents.
# In such cases, the IDF for these terms is set to 0.
# This feature can be used by passing the minDocFreq value to the IDF constructor.
#idfIgnore = IDF(minDocFreq=2).fit(tf)
#tfidfIgnore = idfIgnore.transform(tf)
# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("data/mllib/sample_svm_data.txt")
parsedData = data.map(parsePoint)
