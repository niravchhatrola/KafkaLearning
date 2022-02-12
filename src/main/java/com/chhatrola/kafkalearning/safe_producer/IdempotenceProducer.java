package com.chhatrola.kafkalearning.safe_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by niv214 on 12/2/22.
 */
public class IdempotenceProducer {

    final KafkaProducer<String, String> producer;

    IdempotenceProducer(){
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES());
    }

    public void send(String topic, String key, String value) throws ExecutionException, InterruptedException {
        System.out.println("Key : "+key);
        producer.send(new ProducerRecord(topic, key, value), (RecordMetadata recordMetadata, Exception e) -> {
            System.out.println("Received new metadata \n" +
                    "Topic : "+recordMetadata.topic() +"\n" +
                    "Partition : "+recordMetadata.partition() +"\n" +
                    "Offset : "+recordMetadata.offset() +"\n" +
                    "Timestamp : "+recordMetadata.timestamp() +"\n"
            );
        })
                .get();   // just to make call sync,  do not use it in production

        producer.flush();
    }

    public static final Properties PRODUCER_PROPERTIES(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer properties
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


        return properties;
    }
}
