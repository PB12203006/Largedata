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

numfeatures=2000
raw_data = sc.textFile("/Users/Jillian/Documents/Python/large_data_pj/data/news_sections_abstract2016.txt")
lines = raw_data.map(lambda line: line.split("  ")).map(lambda line: (line[0]," ".join(line[1:])))
sentenceData = spark.createDataFrame(lines,["label", "sentence"])

from pyspark.ml.feature import HashingTF, IDF, Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
wordsData = tokenizer.transform(sentenceData)
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=numfeatures)
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
(train, test) = labeleddata.randomSplit([0.999, 0.001])

"""
Multiclass Perceptron
"""

dataset = test

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
print trainnum
"""
for i in range(numclasses):
    labelforone = trainlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
    models.append(PerceptronforRDD(numFeatures=numfeatures))

    # Combine positive data with negative data on balance proportion
    dataYES = labelforone.zip(traindata).filter(lambda x:x[0]==1)
    count = dataYES.count()
    frac = float(count)/float(trainnum-count)
    dataNO = labelforone.zip(traindata).filter(lambda x: x[0]==-1).sample(False,frac,1)
    dataCOM = dataYES.union(dataNO)
    data = dataCOM.map(lambda x: x[1])
    label = dataCOM.map(lambda x: x[0])

    # Train perceptron models for each class
    models[i].PerceptronBatch(data,label)
    models[i].AveragePerceptron(data,label,MaxItr=1)

    # save the parameters of perceptron model for each class
    parameters = {"w":models[i].w.tolist(),"b":models[i].b,"u_avg":models[i].u_avg.tolist(),"beta_avg":models[i].beta_avg,"count_avg":models[i].count_avg}
    model_param[category[i]]=parameters

    # training error rate and confution matrix
    preds = models[i].Predict(traindata)
    err = labelforone.zip(preds).map(lambda (x,y):1.0*(x!=y)+0.0).sum()
    errYES = labelforone.zip(preds).map(lambda (x,y):1.0*(x!=y and x==1)+0.0).sum()
    trueYES = labelforone.map(lambda x: 1.0*(x==1)+(0.0)*(x==-1)).sum()
    errorRate=float(err)/float(testnum)
    errors.append(Row(category=category[i],truePositive=int(trueYES-errYES),falseNegative=int(errYES), \
                    falsePositive=int(err-errYES),trueNegative=int(testnum-err-trueYES+errYES),errorRate=errorRate))
    #print "error rate of",category[i],"category is:", errorRate

errDF = sc.parallelize(errors).toDF()
errDF.show()
"""

# Write the model to the perceptronModels.json file to save the trained model
"""
json_file = "perceptronModels.json"
with open(json_file, 'w') as outfile:
    json.dump(model_param, outfile)
print "write models parameters to file complete"
"""

# Test the MulticlassPerceptron Class: train/ save/ load/ predict
"""
multiclassperceptron = MulticlassPerceptron(dictionary=dictionary,category=category)
print "training................"
models = multiclassperceptron.train(traindata,trainlabels.map(lambda x: category[int(x)]))
print "predicting.............."
print multiclassperceptron.predict(traindata)
"""

#multiclassperceptron.save("test0.json")

loadperceptron0 = MulticlassPerceptron(dictionary=dictionary,category=category)
loadmodels = loadperceptron0.load("perceptronModels.json",average=True)
print loadperceptron0.predict(traindata)

trainlabels = dataset.select('label').rdd.map(lambda row: category[int(row.label)])#.collect()
fbtest = MulticlassPerceptron(dictionary=dictionary,category=category)
models = fbtest.train(traindata,trainlabels,source="Feedback")
print fbtest.predict(traindata)
fbtest.save("test1.json")

loadperceptron1 = MulticlassPerceptron(dictionary=dictionary,category=category)
loadmodels = loadperceptron1.load("test1.json",average=True)
print loadperceptron1.predict(traindata)
