package com.myntra.kafkarun.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomBeans {
	@Bean
	public NormalConsumer normalConsumer() {
		return new NormalConsumer();
	}
}
