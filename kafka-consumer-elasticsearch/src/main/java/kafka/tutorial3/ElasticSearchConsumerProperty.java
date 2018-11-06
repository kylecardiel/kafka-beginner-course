package kafka.tutorial3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class ElasticSearchConsumerProperty {

    static Properties createTwitterProducerProperty() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-elasticsearch";
        String offsetReset = "earliest";
        String autoCommit = "false";

        // create configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        return properties;
    }

}