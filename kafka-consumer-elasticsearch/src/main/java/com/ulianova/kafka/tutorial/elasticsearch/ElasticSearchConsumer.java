package com.ulianova.kafka.tutorial.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        String hostname = args[0];
        String username = args[1];
        String password = args[2];

        RestHighLevelClient client = createClient(hostname, username, password);


        KafkaConsumer<String, String> consumer = createConsumer(TutorialConstants.TOPIC_TWITTER_TWEETS);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received {} records from partitions {}",
                    records.count(),
                    records.partitions().stream().map(tp -> tp.partition()).collect(Collectors.toSet()));

            BulkRequest elasticBulkRequest = new BulkRequest();

            records.forEach(record -> {
                try {
                    MockTweetDto mockTweetDto = objectMapper.readValue(record.value(), MockTweetDto.class);
                    String id = mockTweetDto.getId();
                    // another id option
                    // record.topic() + "_" + record.partition() + "_" + record.offset();

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id // to make consumer idempotent
                    ).source(record.value(), XContentType.JSON);
                    elasticBulkRequest.add(indexRequest);
                } catch (IOException e) {
                    logger.error("Failed to index a message", e);
                }
            });

            if (!records.isEmpty()) {
                logger.info("Sending bulk request to elastic");
                BulkResponse elasticBulkResponse = client.bulk(elasticBulkRequest, RequestOptions.DEFAULT);
                List<String> ids = Arrays.stream(elasticBulkResponse.getItems()).map(response -> response.getId()).collect(Collectors.toList());
                logger.info("Sent records with ids {}", ids);
                logger.info("Committing offsets");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000); // just a pause for a better demo
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //client.close();
    }

    public static RestHighLevelClient createClient(String hostname, String username, String password) {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, TutorialConstants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TutorialConstants.ELASTIC_GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(10));

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        return consumer;
    }
}
