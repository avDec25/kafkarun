package com.myntra.kafkarun.functionality;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PartitionTransferrer implements Runnable {

	public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String CONSUMER_COMMIT_RETRIES = "2";
	public static final String ACKS = "all";
	public static final String AUTO_COMMIT = "false";
	public static final String BROADCAST_TOPIC = "/topic/partition-transfer";
	Gson gson = new GsonBuilder().disableHtmlEscaping().create();
	public static String CONSUME_TOPIC;
	public static int CONSUME_PARTITION;
	public static int COMMIT_BATCH;

	Integer processed;
	TopicPartition topicPartition;


	private KafkaConsumer<String, Object> consumer;

	AtomicBoolean shouldStop;
	PartitionTransferRequest request;
	KafkaTemplate<String, Object> producer;
	SimpMessagingTemplate messagingTemplate;

	public PartitionTransferrer(AtomicBoolean shouldStop, PartitionTransferRequest request, SimpMessagingTemplate messagingTemplate) {
		this.shouldStop = shouldStop;
		this.request = request;
		this.processed = 0;
		this.messagingTemplate = messagingTemplate;

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
		emit("Starting Partition Transfer");
		emit(String.format("Consume from Kafka: %s ; Produce to Kafka: %s",
				request.consumeFromKafka, request.produceToKafka
		));
		emit(String.format("Consume from topic: %s ; Consume from Partition: %d ; Produce to topic: %s ; Produce to Partition %d ",
				request.consumeFromTopic,
				request.consumeFromPartition,
				request.produceToTopic,
				request.produceToPartition
		));
		while (!shouldStop.get()) {
			ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
			if (records.isEmpty()) {
				emit("No more records to consume");
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
		String message = String.format(
				"Topic: %s, Partition: %d, Lag: %s, Last Committed: %d ",
				request.consumeFromTopic,
				request.consumeFromPartition,
				lag.isEmpty() ? "Unknown" : lag.getAsLong(),
				consumer.position(topicPartition)
		);
		emit(message);
	}

	void gracefulShutDown() {
		System.out.println("======================================================");
		System.out.println("Initiated: Graceful shutdown of PartitionTransferrer");
		System.out.println("======================================================");
		consumer.commitSync();
		printDetails();
		System.out.println("======================================================");
		consumer.close();
		emit("Process Shutdown.");
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
				emit("Failed to push record: " + producerRecord);
			}

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				processed += 1;
			}
		});
	}

	private void emit(String message) {
		JsonObject response = new JsonObject();
		response.addProperty("timestamp", "" + new Date());
		response.addProperty("message", message);
		messagingTemplate.convertAndSend(BROADCAST_TOPIC, gson.toJson(response));
		log.info(message);
	}

}

