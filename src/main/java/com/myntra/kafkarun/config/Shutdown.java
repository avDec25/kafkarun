package com.myntra.kafkarun.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Slf4j
@Component
public class Shutdown {

	@Autowired
	NormalConsumer normalConsumer;

	@PreDestroy
	public void destroy() {
		log.info("Shutdown Initiated for Kafka-Run");
	}
}
