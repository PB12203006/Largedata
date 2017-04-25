from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.mllib.feature import HashingTF as HashingTFmllib
from pyspark.mllib.feature import IDF as IDFmllib
from pyspark.ml.feature import HashingTF, IDF, Tokenizer
from perceptron import PerceptronforRDD
import json

import Push_to_ES
from perceptronMulticlass import MulticlassPerceptron
#initialize
sc=pyspark.SparkContext(appName="akftest")
sc.setLogLevel("WARN")
ssc=pyspark.streaming.StreamingContext(sc,5)#second argument is the period of pulling data
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
perceptronmodels = multiperceptron.load("/home/pb12203006/Documents/Largedata_with_Princess/classification/perceptronModels.json")

'''
init_rdd=sc.emptyRDD()
init_rdd.cache()
#accumulate a large rdd and output
def process(rdd):
	global init_rdd
	init_rdd=init_rdd.union(rdd)
	init_rdd.cache()
	if init_rdd.count()>=5:
		print '\n\n\n\n\n begin learning \n\n\n\n\n'
		print init_rdd.collect()
		init_rdd=sc.emptyRDD()
		init_rdd.cache()
'''

def Tfidfmllib(data):
	training_raw = sc.parallelize(data)
	labels = training_raw.map(
    	lambda doc: doc["label"],  # Standard Python dict access
    	preservesPartitioning=True # This is obsolete.
	)
	# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
	# First to compute the IDF vector and second to scale the term frequencies by IDF.
	tf = HashingTFmllib(numFeatures=2000).transform( ## Use much larger number in practice
    	training_raw.map(lambda doc: doc["text"].split(),
    	preservesPartitioning=True))
	#tf.cache()
	idf = IDFmllib().fit(tf)
	tfidf = idf.transform(tf)
	return tfidf
'''
def Tf(data):
	training_raw = sc.parallelize(data)
	labels = training_raw.map(
    	lambda doc: doc["label"],  # Standard Python dict access
    	preservesPartitioning=True # This is obsolete.
	)
	# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
	# First to compute the IDF vector and second to scale the term frequencies by IDF.
	tf = HashingTF(numFeatures=200).transform( ## Use much larger number in practice
    	training_raw.map(lambda doc: doc["text"].split(),
    	preservesPartitioning=True))
	return [tf,labels]
def ClassifybyPerceptron(tf,label):
	[w,b] = model.AveragePerceptron(tf, label)
	return [model,w,b]
'''

def Tfmllib(data):
	training_raw = sc.parallelize(data)
	labels = training_raw.map(
    	lambda doc: doc["label"],  # Standard Python dict access
    	preservesPartitioning=True # This is obsolete.
	)
	# While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
	# First to compute the IDF vector and second to scale the term frequencies by IDF.
	tf = HashingTFmllib(numFeatures=2000).transform( ## Use much larger number in practice
    	training_raw.map(lambda doc: doc["text"].split(),
    	preservesPartitioning=True))
	return [tf,labels]

def PreferencePerceptron(tf,label):
	[w,b] = model.AveragePerceptron(tf, label)
	return [model,w,b]

def process_feedback_A(rdd):
	if rdd.count()!=0:
		rdd = rdd.map(lambda x: x[-1])
		print rdd.collect()
		for element in rdd.collect():
			data = json.loads(element)
			tfml = Tfml_feedback_A(data)
			category = sc.parallelize(data).map(lambda x:x["category"])
			print "count perceptron classifier:", multiperceptron.models[0].count_avg
			models =multiperceptron.train(tfml,category)
			print "actrual category:", data[0]["category"]
			print "count perceptron classifier after training:", models[0].count_avg
			print "predict tweet category:", multiperceptron.predict(tfml)
			print "\n"

			

#if rdd nonempty, do something
def process_feedback_B(rdd):
	if rdd.count()!=0:
		rdd = rdd.map(lambda x: x[-1])
		print rdd.collect()
		for element in rdd.collect():
			data=json.loads(element)
			print '\n\n\n\n\n\n\n'
		#	print('Training data:\n',data)
			print('\n')
			[tfmllib, labels] = Tfmllib(data)
		#	print('Training data after TF:\n',[tf.take(10),labels.take(10)])
			print('\n')
			[model,w,b] = PreferencePerceptron(tfmllib,labels)
			print('Perceptron weight:\n',[w,b])
			print('\n')
			errrate = model.PredictErrrate(tfmllib,labels)
			print('Training error rate:', errrate)
			print '\n\n\n\n\n\n'
			#call any function you like

def Tfml_feedback_A(data):
	raw_data = sc.parallelize(data)
	lines = raw_data.map(lambda doc: doc["text"]).map(lambda line: Row(sentence=line))
	sentenceData = lines.toDF()
	#sentenceData.show()
	tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
	wordsData = tokenizer.transform(sentenceData)
	hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
	featurizedData = hashingTF.transform(wordsData)
	tf = featurizedData.select('features').rdd.map(lambda x:x.features)
	return tf

def Tfml_tweets(data):
	raw_data = sc.parallelize(data)
	lines = raw_data.map(lambda doc: doc["tweet_text"]).map(lambda line: Row(sentence=line))
	sentenceData = lines.toDF()
	#sentenceData.show()
	tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
	wordsData = tokenizer.transform(sentenceData)
	hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2000)
	featurizedData = hashingTF.transform(wordsData)
	tf = featurizedData.select('features').rdd.map(lambda x:x.features)
	return tf

def process_tweets(rdd):
	if rdd.count()!=0:
		rdd = rdd.map(lambda x: x[-1])
		#print 'collect'
		#print rdd.collect()
		for element in rdd.collect():
			data = json.loads(element)
			tfml = Tfml_tweets(data)
		#	print 'After Tf'
		#	print tfml.collect()
			labels=model.Predict(tfml)
			print labels.collect()[0]
			#categories = tweets_category_predict_nb.predictTweetCategNB(tfml.collect(),sc)
			#print categories
			categories = multiperceptron.predict(tfml)
			#print categories
			print categories
			for category in categories:
				if labels.collect()[0]>0:
					category=category+'_like'
				else:
				#Push_to_ES.push('tweets',,json.dumps(data[0]))
					category=category+'_dislike'
				Push_to_ES.push('tweets',category,json.dumps(data[0]))

kafkaparams={
	"zookeeper.connect":"localhost:2181",
	"group.id":"my-group",
	"zookeeper.connection.timeout.ms":"10000",
	"metadata.broker.list": "localhost:9092"
}

Stream_feedback_B = KafkaUtils.createDirectStream(ssc,topic_1,kafkaparams)
Stream_feedback_B.pprint()
Stream_feedback_A = KafkaUtils.createDirectStream(ssc,topic_3,kafkaparams)
Stream_feedback_A.pprint()
Stream_rawtweets = KafkaUtils.createDirectStream(ssc,topic_2,kafkaparams)
Stream_feedback_A.foreachRDD(lambda k: process_feedback_A(k))
Stream_feedback_B.foreachRDD(lambda k: process_feedback_B(k))
Stream_rawtweets.pprint()
Stream_rawtweets.foreachRDD(lambda k: process_tweets(k))

#KafkaStream.count().pprint()
ssc.start()
ssc.awaitTermination()