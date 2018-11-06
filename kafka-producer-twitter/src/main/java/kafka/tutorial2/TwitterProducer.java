package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    /**  Had to create the topic first in Kafka:
      bin/kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1 */
    private String TweetTopic = "twitter_tweets";
//    private List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");
    private List<String> terms = Lists.newArrayList("bitcoin");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("Setup");

        KafkaProducer<String, String> producer = createKafkaProducer();

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create a twitter client
        Client client = createTwitterClient(msgQueue,terms);
        client.connect();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("stopping application...");
            logger.info("shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("done!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(hasValue(msg)){
                logger.info(msg);

                ProducerRecord<String,String> record = new ProducerRecord(TweetTopic,null, msg);

                producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(hasValue(exception)){
                            logger.error("Error while Producing: ", exception);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue, List<String> terms){
        TwitterClient client = new TwitterClient();
        return client.create(msgQueue,terms);
    }

    private KafkaProducer<String,String> createKafkaProducer(){
        Properties properties = TwitterProducerProperty.createTwitterProducerProperty();
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private boolean hasValue(Object o){
        return o != null;
    }
}
