package com.chhatrola.kafkalearning.assign_seek;

import java.util.concurrent.ExecutionException;

/**
 * In this example we have simply created
 * one produer to produce
 * one consumer to consume
 *
 * Created by niv214 on 6/2/22.
 */
public class ProducerRunner {

    private static final String TOPIC = "first_topic";
    private static final String KEY = "Key_";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Producer producer = new Producer();
        for(int i=0; i<=10; i++){
            producer.send(TOPIC, KEY+(i%2), "Hello"+i);
        }
        System.out.println("------------");
    }
}
