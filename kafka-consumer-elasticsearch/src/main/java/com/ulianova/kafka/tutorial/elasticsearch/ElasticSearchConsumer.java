package com.ulianova.kafka.tutorial.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException {

        String hostname = args[0];
        String username = args[1];
        String password = args[2];

        RestHighLevelClient client = createClient(hostname, username, password);


        KafkaConsumer<String, String> consumer = createConsumer(TutorialConstants.TOPIC_TWITTER_TWEETS);
        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            records.forEach(record -> {
                try {
                    MockTweetDto mockTweetDto = objectMapper.readValue(record.value(), MockTweetDto.class);
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            mockTweetDto.getId() // to make consumer idempotent
                    ).source(record.value(), XContentType.JSON);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    String id = indexResponse.getId();
                    logger.info(id); // check by GET /twitter/tweets/{id}
                    Thread.sleep(1000L);
                } catch (IOException e) {
                    logger.error("Failed to index a message", e);
                } catch (InterruptedException e) {
                    logger.error("Unexpected exception", e);
                }
            });

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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        return consumer;
    }
}
