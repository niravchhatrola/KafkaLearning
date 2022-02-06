package com.chhatrola.kafkalearning.multiple_consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by niv214 on 6/2/22.
 */
@Slf4j
public class Producer {

    final KafkaProducer<String, String> producer;

    Producer(){
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
        return properties;
    }
}
