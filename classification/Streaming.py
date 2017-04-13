from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming
import pyspark
import json
from pyspark.mllib.feature import HashingTF, IDF
from perceptron import PerceptronforRDD
#initialize
sc=pyspark.SparkContext(appName="akftest")
sc.setLogLevel("WARN")
ssc=pyspark.streaming.StreamingContext(sc,5)#second argument is the period of pulling data
topic=["test"] #the topic of kafka
model = PerceptronforRDD(200)

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
def Tfidf(data):
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
	#tf.cache()
	idf = IDF().fit(tf)
	tfidf = idf.transform(tf)
	return tfidf

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


#if rdd nonempty, do something
def process(rdd):
	if rdd.count()!=0:
		print '\n\n\n\n\n\n\n'
		data = json.loads(rdd.collect()[0][1])
		print('Training data:\n',data)
		print('\n')
		[tf, labels] = Tf(data)
		print('Training data after TF:\n',[tf.take(10),labels.take(10)])
		print('\n')
		[model,w,b] = ClassifybyPerceptron(tf,labels)
		print('Perceptron weight:\n',[w,b])
		print('\n')
		errrate = model.PredictErrrate(tf,labels)
		print('Training error rate:', errrate)
		print '\n\n\n\n\n\n'
		#call any function you like

kafkaparams={
	"zookeeper.connect":"localhost:2181",
	"group.id":"my-group",
	"zookeeper.connection.timeout.ms":"10000",
	"metadata.broker.list": "localhost:9092"
}

KafkaStream = KafkaUtils.createDirectStream(ssc,topic,kafkaparams)
#KafkaStream.pprint()
#operate rdd
KafkaStream.foreachRDD(lambda k: process(k))
#KafkaStream.count().pprint()
ssc.start()
ssc.awaitTermination()
