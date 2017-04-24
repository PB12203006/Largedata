from pyspark import SparkContext
import numpy as np
from pyspark.sql import SparkSession
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from perceptron import PerceptronforRDD
from tweets_category_predict_perceptron import predictTweetCategPerceptron

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
labeleddata.cache()
(train, test) = labeleddata.randomSplit([0.8, 0.2])
"""
Multiclasses Perceptron
"""

dataset = labeleddata

traindata = dataset.select('features').rdd.map(lambda row: row.features)
trainlabels = dataset.select('label').rdd.map(lambda row: row.label)#.collect()

numclasses = 19
models = []
num = trainlabels.count()
model_para = {}

for i in range(numclasses):
    models.append(PerceptronforRDD(numFeatures=2000))
    labelforone = trainlabels.map(lambda x: 1.0*(x==i)+(-1.0)*(x!=i))
    dataYES = labelforone.zip(traindata).filter(lambda x:x[0]==1)
    #print dataYES.take(10)
    count = dataYES.count()
    frac = float(count)/float(num-count)
    for j in range(3):
        dataNO = labelforone.zip(traindata).filter(lambda x: x[0]==-1).sample(False,frac,1)
        dataCOM = dataYES.union(dataNO)
        data = dataCOM.map(lambda x: x[1])
        label = dataCOM.map(lambda x: x[0])
        models[i].AveragePerceptron(data,label)
    parameters = {"w":models[i].w.tolist(),"b":models[i].b,"u_avg":models[i].u_avg.tolist(),"beta_avg":models[i].beta_avg,"count_avg":models[i].count_avg}
    model_para[category[i]]=parameters
    #preds = models[i].Predict(traindata)
#print json.dumps(model_para, indent=4)
txt_file = "perceptronModels.json"
with open(txt_file, 'w') as outfile:
    json.dump(model_para, outfile)
print "write models parameters to file complete"

"""
categoryPredict=predictTweetCategPerceptron(traindata)
print categoryPredict
"""
