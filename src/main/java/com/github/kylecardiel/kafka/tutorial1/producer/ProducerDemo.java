package com.github.kylecardiel.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    //psvm tab shorthand
    public static void main(String[] args) {

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
        producer.send(record);

//      4 send data - async
        producer.flush();
        producer.close();
    }

}
