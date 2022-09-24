package com.myntra.kafkarun.functionality;

import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionTransferrer implements Runnable {

	public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String CONSUMER_COMMIT_RETRIES = "2";
	public static final String ACKS = "all";
	public static final String AUTO_COMMIT = "false";
	public static String CONSUME_TOPIC;
	public static int CONSUME_PARTITION;
	public static int COMMIT_BATCH;

	Integer processed;
	TopicPartition topicPartition;


	private KafkaConsumer<String, Object> consumer;

	AtomicBoolean shouldStop;
	PartitionTransferRequest request;
	KafkaTemplate<String, Object> producer;

	public PartitionTransferrer(AtomicBoolean shouldStop, PartitionTransferRequest request) {
		this.shouldStop = shouldStop;
		this.request = request;
		this.processed = 0;

		CONSUME_TOPIC = request.consumeFromTopic;
		CONSUME_PARTITION = request.consumeFromPartition;
		COMMIT_BATCH = request.commitBatch;

		// should be initialized only after all other properties are set.
		initializeConsumer(request);
		initializeProducer(request);
	}


	private void initializeConsumer(PartitionTransferRequest request) {
		Properties consumerProperties = new Properties();
		consumerProperties.setProperty("bootstrap.servers", request.consumeFromKafka);
		consumerProperties.setProperty("key.deserializer", KEY_DESERIALIZER);
		consumerProperties.setProperty("value.deserializer", VALUE_DESERIALIZER);
		consumerProperties.setProperty("retries", CONSUMER_COMMIT_RETRIES);
		consumerProperties.setProperty("client.id", "kafkaRun-pt-consumer-" + System.currentTimeMillis());
		consumerProperties.setProperty("acks", ACKS);
		consumerProperties.setProperty("enable.auto.commit", AUTO_COMMIT);
		consumerProperties.setProperty("group.id", request.consumerGroupId);
		consumer = new KafkaConsumer<>(consumerProperties);
		topicPartition = new TopicPartition(CONSUME_TOPIC, CONSUME_PARTITION);
		consumer.assign(Collections.singleton(topicPartition));
	}

	private void initializeProducer(PartitionTransferRequest request) {
		Map<String, Object> producerProperties = new HashMap<>();
		producerProperties.put("bootstrap.servers", request.produceToKafka);
		producerProperties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("client.id", "kafkaRun-pt-producer-" + System.currentTimeMillis());
		ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties);
		producer = new KafkaTemplate<>(producerFactory);
	}


	@SneakyThrows
	@Override
	public void run() {
		while (!shouldStop.get()) {
			ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
			if (records.isEmpty()) {
				System.out.println("-----------------");
				System.out.println("No more records");
				System.out.println("-----------------");
				break;
			}
			for (ConsumerRecord<String, Object> record : records) {
				processRecord(record);
				if (processed % COMMIT_BATCH == 0) {
					consumer.commitAsync();
					printDetails();
				}
			}
		}
		gracefulShutDown();
	}

	private void printDetails() {
		OptionalLong lag = consumer.currentLag(topicPartition);
		System.out.printf(
				"Topic: %s, Partition: %d, Lag: %s, Last Committed: %d\n",
				request.consumeFromTopic,
				request.consumeFromPartition,
				lag.isEmpty() ? "Unknown" : lag.getAsLong(),
				consumer.position(topicPartition)
		);
	}

	void gracefulShutDown() {
		System.out.println("======================================================");
		System.out.println("Initiated: Graceful shutdown of PartitionTransferrer");
		System.out.println("======================================================");
		consumer.commitSync();
		printDetails();
		System.out.println("======================================================");
		consumer.close();
	}

	void processRecord(ConsumerRecord<String, Object> record) {
		ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
				request.produceToTopic,
				request.produceToPartition,
				record.key(),
				record.value()
		);
		producer.send(producerRecord).addCallback(new ListenableFutureCallback<>() {
			@Override
			public void onFailure(@NonNull Throwable ex) {
				ex.printStackTrace();
				System.out.println("Failed to push message: " + producerRecord);
			}

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				processed += 1;
			}
		});
	}
}

