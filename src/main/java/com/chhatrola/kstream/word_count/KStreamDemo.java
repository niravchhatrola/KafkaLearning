package com.chhatrola.kstream.word_count;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/**
 * This demo will consume string and will count number of occurences of word.
 * Created by niv214 on 27/2/22.
 *
 * Create input and output topic
 kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-input
 kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic word-count-output

 Produce message to input topic

 That will be consume by kstream demo application and being transformed/processed. than will be stream to output topic.

 Consume output topic using below command to see output.
 kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count-output --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
public class KStreamDemo {

    private static final String INPUT_TOPIC = "word-count-input";
    private static final String OUTPUT_TOPIC = "word-count-output";

    public static void main(String[] args) {
        new KStreamDemo().consume();
    }

    public void consume(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Prepare stream
        KStream<String, String> stream = streamsBuilder.stream(INPUT_TOPIC);

        // lower case value
        KTable<String, Long> wordCountTable = stream.mapValues(value -> value.toLowerCase())

                // map values from line to word
                .flatMapValues(value -> Arrays.asList(value.split(" ")))

                // key was null so lets selct value as key
                .selectKey((key, value) -> value)
                //group it count
                .groupByKey()
                .count();

        wordCountTable.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), CONSUMER_PROPERTIES());

        kafkaStreams.start();

        System.out.println(kafkaStreams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

    }

    public static final Properties CONSUMER_PROPERTIES(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "work-count-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;


    }
}
