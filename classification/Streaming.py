from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming
import pyspark
#initialize
sc=pyspark.SparkContext(appName="akftest")
sc.setLogLevel("WARN")
ssc=pyspark.streaming.StreamingContext(sc,5)#second argument is the period of pulling data
topic=["test"] #the topic of kafka

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
#if rdd nonempty, do something
def process(rdd):
	if rdd.count()!=0:
		print '\n\n\n\n\n\n\n'
		print rdd.collect()
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
