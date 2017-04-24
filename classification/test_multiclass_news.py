from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from perceptron import PerceptronforRDD, PerceptronOVRforDF



sc = SparkContext()
spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/news_sections_abstract2016.txt")
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
(train, test) = labeleddata.randomSplit([0.8, 0.2])
labeleddata.cache()
"""
Multiclass Perceptron
"""

dataset = train#.randomSplit([0.8, 0.2])
dataset.cache()
'''
labels = test.select('label').rdd.map(lambda row: row.label).collect()
modelmulti = PerceptronOVRforDF(numFeatures=2000, numClasses=19)
[w,b] = modelmulti.PerceptronRDDOVR(dataset)
#print "w:", w.take(5)
#print "b:", b.collect()
preds = modelmulti.Predict(test.select('features').rdd.map(lambda row: row.features)).collect()
err = [1*(labels[i] in preds[i]) for i in range(len(labels))]
errrate = float(sum(err))/float(len(labels))
print "Perceptron error rate:", errrate
'''

"""
traindata = dataset.select('features').rdd.map(lambda row: row.features)
trainlabels = dataset.select('label').rdd.map(lambda row: row.label).collect()
models = []
e = 0
numclasses = 19
models = [PerceptronforRDD(numFeatures=2000)]*19
for i in range(numclasses):
    model = models[i]
    labelforone = sc.parallelize([1.0*(trainlabels[j]==i)+(-1.0)*(trainlabels[j]!=i) for j in range(len(trainlabels))])
    model.PerceptronBatch(traindata,labelforone)
    preds = model.Predict(traindata)
    pred = preds.collect()
    label = labelforone.collect()
    errrate = sum([1.0*(pred[j]!=label[j])+0*(pred[j]==label[j]) for j in range(len(label))])/float(len(pred))
    print "error rate:",i, errrate
    e = e + errrate
print "Overall error rate for perceptron:", e
"""

"""
NaiveBayes
"""

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

model = nb.fit(train)

# select example rows to display.
predictions = model.transform(test)
#predictions.show()

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))


"""
Decision Tree
"""
"""
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# Index labels, adding metadata to the label column.
# Fit on whole dataset to include all labels in index.
data = labeleddata
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=1000).fit(data)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = data.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)
"""

"""
Random Forest
"""
"""
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", numTrees=10)

# Convert indexed labels back to original labels.
labelConverter = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                               labels=labelIndexer.labels)

# Chain indexers and forest in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, rf, labelConverter])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

rfModel = model.stages[2]
print(rfModel)  # summary only
"""
"""
One-vs-Rest classifier (a.k.a. One-vs-All)
"""
"""
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# instantiate the base classifier.
lr = LogisticRegression(maxIter=100, tol=1E-6, fitIntercept=True)

# instantiate the One Vs Rest Classifier.
ovr = OneVsRest(classifier=lr)

# train the multiclass model.
ovrModel = ovr.fit(train)

# score the model on test data.
predictions = ovrModel.transform(test)

# obtain evaluator.
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")

# compute the classification error on test data.
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
"""
"""
Multilayer perceptron classifier
"""
"""
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#splits = data.randomSplit([0.6, 0.4], 1234)
#train = splits[0]
#test = splits[1]
# specify layers for the neural network:
# input layer of size 4 (features), two intermediate of size 5 and 4
# and output of size 3 (classes)
layers = [2000, 1000, 1000, 19]

# create the trainer and set its parameters
trainer = MultilayerPerceptronClassifier(maxIter=5, layers=layers, blockSize=128, seed=1234)

# train the model
model = trainer.fit(train)

# compute accuracy on the test set
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
print("Test set accuracy = " + str(evaluator.evaluate(predictionAndLabels)))
"""