package com.github.kylecardiel.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    //psvm tab shorthand
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
//      3 steps to creating producer

//      1 create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//      2 create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        for(int i = 0 ; i < 10 ; i++){


        String topic = "first_topic";
        String value = "Hello World " + Integer.toString(i);
        String key = "id_" + Integer.toString(i);

//      3 create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: "+ key);

//      4 send data - async
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes every time a record is successfully sent or exception is thrown
                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp() + "\n");
                    } else {
                        logger.error("Errosr while Producing: ", exception);
                    }
                }
            }).get();   //block the send() to make it synchronous - dont do in prod
        }
//      4 send data - async
        producer.flush();
        producer.close();
    }

}
