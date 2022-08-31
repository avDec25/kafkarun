package com.myntra.kafkarun;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkarunApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkarunApplication.class, args);
	}

}
