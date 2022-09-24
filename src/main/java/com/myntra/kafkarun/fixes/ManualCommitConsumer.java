package com.myntra.kafkarun.fixes;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ManualCommitConsumer implements Runnable {

	public static final String KFK_SERVERS = "localhost:9092";
	public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String CONSUMER_COMMIT_RETRIES = "0";
	public static final String ACKS = "all";
	public static final String AUTO_COMMIT = "false";
	public static final String CONSUME_TOPIC = "dolphin";
	public static final int CONSUME_PARTITION = 0;
	public static final String GROUP = "ocean-consumer-group";
	public static final int COMMIT_BATCH = 10;

	static Properties kfkProps = new Properties();

	static {
		kfkProps.setProperty("bootstrap.servers", KFK_SERVERS);
		kfkProps.setProperty("key.deserializer", KEY_DESERIALIZER);
		kfkProps.setProperty("value.deserializer", VALUE_DESERIALIZER);
		kfkProps.setProperty("retries", CONSUMER_COMMIT_RETRIES);
		kfkProps.setProperty("acks", ACKS);
		kfkProps.setProperty("enable.auto.commit", AUTO_COMMIT);
		kfkProps.setProperty("group.id", GROUP);
	}

	private final KafkaConsumer<String, String> consumer;

	AtomicBoolean shouldStop;
	public ManualCommitConsumer(AtomicBoolean shouldStop) {
		this.shouldStop = shouldStop;
		consumer = new KafkaConsumer<>(kfkProps);
	}

	@SneakyThrows
	@Override
	public void run() {
		TopicPartition topicPartition = new TopicPartition(CONSUME_TOPIC, CONSUME_PARTITION);
		consumer.assign(Collections.singleton(topicPartition));

		int processed = 0;
		while (!shouldStop.get()) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf(
						"Topic: %s, Partition: %d, Key: %s, Value: %s%n",
						record.topic(),
						record.partition(),
						record.key(),
						record.value()
				);
				processed += 1;
				if (processed % COMMIT_BATCH == 0) {
					consumer.commitAsync();
					System.out.println("Committed offset: " + consumer.position(topicPartition));
				}

				if (shouldStop.get()) {
					System.out.println("================================================");
					System.out.println("Initiated: Graceful shutdown of consumer");
					System.out.println("================================================");
					consumer.commitSync();
					System.out.printf("Processed %d records\n", processed);
					System.out.println("Last Committed offset: " + consumer.position(topicPartition));
					System.out.println("================================================");
					consumer.close();
				}
			}
		}
	}
}
