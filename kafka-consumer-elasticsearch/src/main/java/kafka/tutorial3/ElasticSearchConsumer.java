package kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        String topic = "twitter_tweets";
        KafkaConsumer<String,String> consumer = createKafkaConsumer(topic);

        RestHighLevelClient client = createClient();

        while (true){

            ConsumerRecords<String,String> records =  consumer.poll(Duration.ofMillis(100));

            Integer recordCount = records.count();
            logger.info("Received " + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String,String> record : records){
//                logger.info("Key: " + record.key() + ", Value: " + record.value());
//                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                String jsonString = record.value();

                // There are 2 strategies from making consumer idempotent id's:
                // 1) Kafka generic ID
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                try{
                    // 2) twitter specific id
                    String tweetId = extractIdFromTweet(record.value());

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            tweetId // to make consumer idempotent
                    ).source(jsonString, XContentType.JSON);

                    bulkRequest.add(indexRequest);

//                IndexResponse indexRepsonse = client.index(indexRequest, RequestOptions.DEFAULT);
//                logger.info(indexRepsonse.getId());
                } catch (NullPointerException e){
                    logger.warn("skipping bad data: " + record.value());
                }
            }

            if(recordCount > 0){
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                logger.info("Committing offsets...");
                consumer.commitSync();;
                logger.info("Offset have been committed");
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }
//        client.close();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static String extractIdFromTweet(String tweetJson) {
        return jsonParser.parse(tweetJson)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
    }

    public static RestHighLevelClient createClient(){
        ElasticSearchClient client = new ElasticSearchClient();
        return client.createRestHighLevelClient();
    }

    public static KafkaConsumer<String, String> createKafkaConsumer(String topic){
        Properties properties = ElasticSearchConsumerProperty.createTwitterProducerProperty();
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
}