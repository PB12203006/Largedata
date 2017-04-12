from pyspark.streaming.kafka import KafkaUtils
import pyspark.streaming
import pyspark

sc=pyspark.SparkContext(appName="akftest")
sc.setLogLevel("WARN")
ssc=pyspark.streaming.StreamingContext(sc,5)#second argument is the period of pulling data
topic=["test"] #the topic of kafka
kafkaparams={
	"zookeeper.connect":"localhost:2181",
	"group.id":"my-group",
	"zookeeper.connection.timeout.ms":"10000",
	"metadata.broker.list": "localhost:9092"
}
KafkaStream = KafkaUtils.createDirectStream(ssc,topic,kafkaparams)
KafkaStream.pprint()
ssc.start()
ssc.awaitTermination()
print 'bye'