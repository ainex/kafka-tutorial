# create topic word-count-input
kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2 --topic word-count-input

# create topic word-count-output
kafka-topics --create --bootstrap-server 127.0.0.1:9092 --replication-factor 1 --partitions 2 --topic word-count-output

# launch a kafka consumer
kafka-console-consumer --bootstrap-server 127.0.0.1:9092 `
--topic word-count-output `
--from-beginning `
--formatter kafka.tools.DefaultMessageFormatter `
--property print.key=true `
--property print.value=true `
--property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer `
--property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# launch a kafka producer
kafka-console-producer --bootstrap-server 127.0.0.1:9092 --topic word-count-input

