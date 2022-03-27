package com.chhatrola.kstream.favorite_color;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * This demo will take input from user of his favorate color like below.
 * Nirav,blue
 * Hardik,red
 * Sneha,yellow
 * Nirav,red
 *
 * Output should be color count like below
 * blue,0
 * red,2
 * yellow,1
 * Created by niv214 on 12/3/22.
 *
 * Steps to setup
 * Create 3 topics as below.
 *  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic favorite-color-topic
 *  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic user-color-topic
 *  kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic color-count-topic
 *
 * start producer to input user and favorite color
 * kafka-console-producer.sh --broker-list localhost:9092 --topic favorite-color-topic
 *
 * start consumer on output topic to see final output.
 * kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic color-count-topic --from-beginning --formatter kafka.tools.DefaultMessageFormatter --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 */
public class FavoriteColorDemo {

    private static final String FAVORITE_COLOR_TOPIC = "favorite-color-topic";
    private static final String USER_COLOR_TOPIC = "user-color-topic";
    private static final String COLOR_COUNT_TOPIC = "color-count-topic";



    public static void main(String[] args) {
        new FavoriteColorDemo().process();
    }

    void process(){

        List<String> COLORS = Arrays.asList("Red", "Green", "Blue");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // read from the stream <Name, Color>
        KStream<String, String> favorateTopicStream = streamsBuilder.stream(FAVORITE_COLOR_TOPIC);

        // filter bad colors as only RGB are allowed :) in our world.
        KStream<String, String> userColorStream = favorateTopicStream.map((key, value) -> new KeyValue<>(value.split(",")[0], value.split(",")[1]))
                .filter((name, color) -> COLORS.contains(color));

        //
        userColorStream.to(USER_COLOR_TOPIC);


        KTable<String, String> userColorTable = streamsBuilder.table(USER_COLOR_TOPIC);

        KTable<String, Long> colorCountTable = userColorTable.groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        colorCountTable.toStream().to(COLOR_COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), CONSUMER_PROPERTIES());

        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    }

    public static final Properties CONSUMER_PROPERTIES(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-color-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }

}
