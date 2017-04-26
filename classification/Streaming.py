from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming
import pyspark
from pyspark.sql import Row,SparkSession
from pyspark.mllib.feature import HashingTF as HashingTFmllib
from pyspark.mllib.feature import IDF as IDFmllib
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from perceptron import PerceptronforRDD
import json
import Push_to_ES
from perceptronMulticlass import MulticlassPerceptron
import itertools
#initialize
sc=pyspark.SparkContext(appName="akftest")
sc.setLogLevel("WARN")
ssc=pyspark.streaming.StreamingContext(sc,10)#second argument is the period of pulling data
topic_1=["feedback_B"] #the topic of kafka
topic_2=["twitterstream_raw"]
topic_3=["feedback_A"]
model = PerceptronforRDD(2000)
spark = SparkSession(sc)
categ = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']
dictionary = {'Art & Design':0,'World':1,'Sports':2,'Fashion & Style':3,'Books':4,'Music':5, \
            'Television':6,'Movies':7,'Technology':8,'Science':9,'Food':10,'Real Estate':11,'Theater':12, \
            'Health':13,'Travel':14,'Education':15,'Your Money':16,'Politics':17,'Economy':18}

multiperceptron = MulticlassPerceptron(numFeatures=2000,numClasses=19,dictionary=dictionary,category=categ)
perceptronmodels = multiperceptron.load("/home/pb12203006/Documents/Largedata_with_Princess/classification/perceptronModels.json",average=False)
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
hashingTFmllib = HashingTFmllib(numFeatures=2000)

def process_feedback_A(rdd):
	if rdd.count()!=0:
		rdd = rdd.map(lambda x: x[-1])
		listall = rdd.collect()
		data = json.loads(listall[0])
		if len(listall)!=1:
			for i in range(1,len(listall)):
				data = data+json.loads(listall[i])
		#print data
		raw_data = sc.parallelize(data)
		print raw_data.first()
		lines = raw_data.map(lambda doc: doc["text"])
		lines = lines.map(lambda line: Row(sentence=line))
		sentenceData = lines.toDF()
		#sentenceData = rdd.map(lambda x: json.loads(x[-1]))
		wordsData = tokenizer.transform(sentenceData)
		featurizedData = hashingTF.transform(wordsData)
		tfml = featurizedData.select('features').rdd.map(lambda x:x.features)
		category = raw_data.map(lambda x:x["category"])
		print "count perceptron classifier:", multiperceptron.models[0].count_avg
		models =multiperceptron.train(tfml,category)
		print "actrual category:", data[0]["category"]
		print "count perceptron classifier after training:", models[0].count_avg
		print "predict tweet category:", multiperceptron.predict(tfml)
		print "\n"

		
			

#if rdd nonempty, do something
def process_feedback_B(rdd):
	if rdd.count()!=0:
		listall = rdd.map(lambda x: x[-1]).collect()
		data = json.loads(listall[0])
		if len(listall)!=1:
			for i in range(1,len(listall)):
				data = data+json.loads(listall[i])
		training_raw = sc.parallelize(data)
		labels = training_raw.map(
    		lambda doc: doc["label"],  # Standard Python dict access
    		preservesPartitioning=True # This is obsolete.
		)
		# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
		# First to compute the IDF vector and second to scale the term frequencies by IDF.
		tfmllib = hashingTFmllib.transform( ## Use much larger number in practice
 		   	training_raw.map(lambda doc: doc["text"].split(),
    		preservesPartitioning=True))
		[w,b] = model.AveragePerceptron(tfmllib,labels)
		#print('Perceptron weight:\n',[w,b])
		errrate = model.PredictErrrate(tfmllib,labels)
		print('Training error rate:', errrate)
		print '\n\n\n\n\n\n'
		

def process_tweets(rdd):
	if rdd.count()!=0:
		rdd = rdd.map(lambda x: x[-1])
		data = rdd.collect()
		#print data[0]
		raw_data = sc.parallelize(data)
		lines = raw_data.map(lambda doc: json.loads(doc)["tweet_text"]).map(lambda line: Row(sentence=line))
		sentenceData = lines.toDF()
		#sentenceData = rdd.map(lambda x: json.loads(x[-1]))
		wordsData = tokenizer.transform(sentenceData)
		featurizedData = hashingTF.transform(wordsData)
		tfml = featurizedData.select('features').rdd.map(lambda x:x.features)
		labels=model.Predict(tfml)
		print labels.collect()
		categoriesAll = multiperceptron.predict(tfml)
		print categoriesAll
		for i in range(len(categoriesAll)):
			for category in categoriesAll[i]:
				if labels.collect()[0]>0:
					category=category+'_like'
				else:
				#Push_to_ES.push('tweets',,json.dumps(data[0]))
					category=category+'_dislike'
				#print json.dumps(data[i])
				Push_to_ES.push('tweets',category,json.dumps(data[i]))


kafkaparams={
	"zookeeper.connect":"localhost:2181",
	"group.id":"my-group",
	"zookeeper.connection.timeout.ms":"10000",
	"metadata.broker.list": "localhost:9092"
}

Stream_feedback_B = KafkaUtils.createDirectStream(ssc,topic_1,kafkaparams)
Stream_feedback_A = KafkaUtils.createDirectStream(ssc,topic_3,kafkaparams)
Stream_rawtweets = KafkaUtils.createDirectStream(ssc,topic_2,kafkaparams)

Stream_feedback_B.pprint()
Stream_feedback_A.pprint()
Stream_rawtweets.pprint()
Stream_feedback_B.foreachRDD(lambda k: process_feedback_B(k))
Stream_feedback_A.foreachRDD(lambda k: process_feedback_A(k))
Stream_rawtweets.foreachRDD(lambda k: process_tweets(k))

ssc.start()
ssc.awaitTermination()