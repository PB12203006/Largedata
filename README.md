# Tweet Inspector

1. Run Zookeeper and Kafka

`$ <PATH_to_Kafka>/bin/zookeeper-server-start.sh config/zookeeper.properties`

`$ <PATH_to_Kafka>/bin/kafka-server-start.sh config/server.properties`

2. Create topics

`$ <PATH_to_Kafka>/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic feedback_A`

`$ <PATH_to_Kafka>/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic feedback_B`

`$ <PATH_to_Kafka>/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic twitterstream_raw`

3. Run web server and tweet producer by python

`$ python Run_application.py`

`$ python Run_producer.py`

4. Run Streaming by spark

`$ <PATH_to_Spark>/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --master local[8] Run_Streaming.py` 

