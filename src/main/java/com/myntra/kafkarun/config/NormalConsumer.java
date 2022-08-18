package com.myntra.kafkarun.config;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

@Slf4j
public class NormalConsumer {
	Properties properties = new Properties();
	Consumer<String, Object> consumer;
	Collection<String> allTopics = new HashSet<>();

	@SneakyThrows
	public NormalConsumer() {
		properties.put("group.id", "kafka-run-consumer-group");
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		properties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		properties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		properties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		properties.put("group.instance.id", "1");
		properties.put("client.id", "kafka-run-consumer");
		properties.put("auto.offset.reset", "earliest");

		String hostname = InetAddress.getLocalHost().getHostName();
		properties.put("client.rack", hostname);

		consumer = new KafkaConsumer<>(properties);
	}

	@PreDestroy
	public void shutdown() {
		log.info("Initiated Consumer Shutdown for Topics: {}", String.join(", ", allTopics));
		consumer.close();
	}

	public void consume(String topicName) {
		if (!allTopics.contains(topicName)) {
			allTopics.add(topicName);
			consumer.subscribe(allTopics);
		}
		try {
			log.info("Polling consumer data");
			ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofSeconds(1));
			for (ConsumerRecord<String, Object> consumerRecord : consumerRecords) {
				log.info("\nConsumed Record:");
				System.out.printf("topic: %s%n", consumerRecord.topic());
				System.out.printf("partition: %s%n", consumerRecord.partition());
				System.out.printf("offset: %s%n", consumerRecord.offset());
				System.out.printf("value: %s%n", consumerRecord.value());
				System.out.println();
			}
		} catch (Exception e) {
			log.error("Failed Consumption; Reason: " + e.getCause());
			e.printStackTrace();
		}
	}

}
