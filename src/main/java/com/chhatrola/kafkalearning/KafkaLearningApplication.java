package com.chhatrola.kafkalearning;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaLearningApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaLearningApplication.class, args);
		new KafkaLearningApplication().execute();
	}

	public void execute(){

	}

}
