package com.myntra.kafkarun.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomBeans {
	@Bean
	public NormalConsumer normalConsumer() {
		return new NormalConsumer();
	}

	@Bean
	public Gson getGson() {
		return new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
	}
}
