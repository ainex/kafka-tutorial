# create topic
kafka-topics --bootstrap-server 127.0.0.1:9092 --create --topic twitter_tweets --partitions 6 --replication-factor 1

# describe topic
kafka-topics --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets --describe

#run console consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 --topic twitter_tweets

#check consumers, offsets and lags
kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group kafka-demo-elasticsearch
