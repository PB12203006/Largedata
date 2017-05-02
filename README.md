# Tweet Inspector

1. Run Zookeeper and Kafka

`$ bin/zookeeper-server-start.sh config/zookeeper.properties`

`$ bin/kafka-server-start.sh config/server.properties`

2. Run web server and tweet producer by python

`$ python Run_application.py`

`$ python Run_producer.py`

3. Run Streaming by spark

`$ <PATH_to_Spark>/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --master local[8] Run_Streaming.py` 

