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

Make sure the terminal is in the directory of file Run_Streaming.py because there are some relative directories in code.

On my computer, each time the computer restarts, running this file will fail three times at the beginning with errors that the topic "feedback_B", "feedback_A" and "twitterstream_raw" does not exist, respectively, but it will run successfully since the fourth time.

`$ <PATH_to_Spark>/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --master local[8] Run_Streaming.py` 

Additionally, the folder classification contains the ML research part of our project, please take a look at the readme.md inside.
