# create topic fav-colour-input
kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2 --topic fav-colour-input

# create topic fav-colour-output
kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2 --topic fav-colour-output

# create topic
kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2 --topic fav-colour-filtered-table

# launch a kafka consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 `
--topic fav-colour-output `
--from-beginning `
--formatter kafka.tools.DefaultMessageFormatter `
--property print.key=true `
--property print.value=true `
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer `
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# launch a kafka producer
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic fav-colour-input `
  --property parse.key=true `
  --property key.separator=","

