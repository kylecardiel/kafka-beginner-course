package com.github.kylecardiel.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    //psvm tab shorthand
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        String bootstrapServers = "127.0.0.1:9092";
//      3 steps to creating producer

//      1 create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//      2 create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

//      3 create producer record
        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic","Hello World!");

//      4 send data - async
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes every time a record is successfully sent or exception is thrown
                if(exception == null){
                    logger.info("Received new metadata. \n" +
                           "Topic: "  + metadata.topic() + "\n" +
                           "Partition: "  + metadata.partition() + "\n" +
                           "Offset: "  + metadata.offset() + "\n" +
                           "timestamp: "  + metadata.timestamp() + "\n");
                } else {
                    logger.error("Errosr while Producing: ", exception);
                }
            }
        });

//      4 send data - async
        producer.flush();
        producer.close();
    }

}
