package com.chhatrola.kafkalearning.multiple_consumer;

import java.util.concurrent.ExecutionException;

/**
 * In this example we have simply created
 * one produer to produce
 * multiple consumer to consume
 * case 1) same group
 *      partitions will be destributed between consumers.
 *
 * case 2) different group
 *      all group will be assigned with seperate partitions and so that same data in both consumer group.
 *
 * Created by niv214 on 6/2/22.
 */
public class ProducerRunner {

    private static final String TOPIC = "first_topic";
    private static final String KEY = "Key_";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer producer = new Producer();
        for(int i=0; i<=10; i++){
            producer.send(TOPIC, KEY+(i%3), "Hey there ... "+i);
        }
        System.out.println("------------");
    }
}
