package com.chhatrola.kafkalearning.multiple_consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by niv214 on 6/2/22.
 */
public class Consumer1 {
    KafkaConsumer<String, String> kafkaConsumer;

    Consumer1(){
        kafkaConsumer = new KafkaConsumer<>(CONSUMER_PROPERTIES());
    }

    public void consume(){
        kafkaConsumer.subscribe(Arrays.asList("first_topic"));

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(10));

            records.forEach(record -> System.out.println("Key : "+ record.key() + " Value : "+record.value() + "\n" +
                    "Partition : "+record.partition() +" Offset : "+record.offset()));

        }
    }

    private static final Properties CONSUMER_PROPERTIES(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        return properties;
    }

    public static void main(String[] args) {
        new Consumer1().consume();
    }
}
