from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession, Row
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')

sc = SparkContext()
spark = SparkSession.builder \
        .master("local") \
        .appName("Word Count") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

# Load news category data
raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/news_sections_abstract2016.txt")
lines = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))
sentenceData = spark.createDataFrame(lines,["label", "sentence"])

# Map sentence data to hashingTF
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
featurizedData = hashingTF.transform(wordsData)
#featurizedData.show()

# Map string labels to integer
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

# Split data into train and test set
labeleddata = data0.select(data0.label.cast("double").alias('label'),'features').na.drop()
labeleddata.cache()
(train, test) = labeleddata.randomSplit([0.8, 0.2])
train.cache()
test.cache()
traincount = train.count()
testcount = test.count()
print "\n"
print "training data set count:", traincount
print "test data set count:", testcount

"""
NaiveBayes
"""
from pyspark.ml.classification import NaiveBayes,NaiveBayesModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from tweets_category_predict_nb import predictTweetCategNB

# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

model = nb.fit(train)
# Save and Load NaiveBayes model
#model.save("/Users/Jillian/Documents/Python/large_data_pj/NaiveBayes_model/")
#sameModel = NaiveBayesModel.load("/Users/Jillian/Documents/Python/large_data_pj/NaiveBayes_model/")

# select example rows to display.
#tt = test.select("features").rdd.map(lambda x: x.features)
# Here, replace tt in tt.map() as testtf
#tt = tt.map(lambda x: Row(features=x)).toDF()
#tt.show()
predictions = model.transform(test)
#predictions.show()

#labels = predictTweetCategNB(,sc)
#print labels[:10]

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print "NaiveBayes Test set accuracy = " + str(accuracy) + "\n"


"""
Decision Tree
"""

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# Train model.  This also runs the indexers.
model = dt.fit(train)

# Make predictions.
predictions = model.transform(test)

# Select example rows to display.
#predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print "DecisionTree Test set accuracy =  " + str(accuracy) + "\n"

#treeModel = model.stages[2]
# summary only
#print(treeModel)

"""
Random Forest
"""
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Train a RandomForest model.
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)

# Train model.  This also runs the indexers.
model = rf.fit(train)

# Make predictions.
predictions = model.transform(test)

# Select example rows to display.
#predictions.select("prediction", "label", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
#rfModel = model.stages[2]
print "RandomForest model:",model # summary only
print "RandomForest Test set accuracy = "+ str(accuracy) + "\n"


"""
One-vs-Rest classifier (a.k.a. One-vs-All)
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
print "One-vs-Rest (LogisticRegression) Test set accuracy = "+ str(accuracy) + "\n"

"""
Multiclass Perceptron
"""
from perceptron import PerceptronforRDD
from perceptronMulticlass import MulticlassPerceptron
dataset = train

traindata = dataset.select('features').rdd.map(lambda row: row.features)
trainlabels = dataset.select('label').rdd.map(lambda row: row.label)

testdataset = test
testdata = testdataset.select('features').rdd.map(lambda row: row.features)
testlabels = testdataset.select('label').rdd.map(lambda row: row.label)

numclasses = 19
models = []
model_param = {}
trainnum = trainlabels.count()
testnum = testlabels.count()
errors = []
for i in range(numclasses):
    labelforone = trainlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
    models.append(PerceptronforRDD(numFeatures=2000))

    # Combine positive data with negative data on balance proportion
    dataYES = labelforone.zip(traindata).filter(lambda x:x[0]==1)
    count = dataYES.count()
    frac = float(count)/float(trainnum-count)
    dataNO = labelforone.zip(traindata).filter(lambda x: x[0]==-1).sample(False,frac,1)
    dataCOM = dataYES.union(dataNO)
    data = dataCOM.map(lambda x: x[1])
    label = dataCOM.map(lambda x: x[0])

    # Train perceptron models for each class
    models[i].AveragePerceptron(data,label)

    # save the parameters of perceptron model for each class
    parameters = {"w":models[i].w.tolist(),"b":models[i].b,"u_avg":models[i].u_avg.tolist(),"beta_avg":models[i].beta_avg,"count_avg":models[i].count_avg}
    model_param[category[i]]=parameters

    # test error rate and confution matrix
    preds = models[i].Predict(testdata)
    testlabelforone = testlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
    err = testlabelforone.zip(preds).map(lambda (x,y):1.0*(x!=y)+0.0).sum()
    errYES = testlabelforone.zip(preds).map(lambda (x,y):1.0*(x!=y and x==1)+0.0).sum()
    trueYES = testlabelforone.map(lambda x: 1.0*(x==1)+(0.0)*(x==-1)).sum()
    errorRate=float(err)/float(testnum)
    errors.append(Row(category=category[i],truePositive=int(trueYES-errYES),falseNegative=int(errYES), \
                    falsePositive=int(err-errYES),trueNegative=int(testnum-err-trueYES+errYES),errorRate=errorRate))
    #print "error rate of",category[i],"category is:", errorRate
print "MulticlassPerceptron accuracy:"
errDF = sc.parallelize(errors).toDF()
errDF.show()
print "\n"
