package com.myntra.kafkarun.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Service
public class PartitionTransferService {
	@Autowired
	Gson gson;

	@SneakyThrows
	public String transferPartition(PartitionTransferRequest request) {
		Properties consumerProperties = new Properties();
		Properties producerProperties = new Properties();

		consumerProperties.put("bootstrap.servers", request.consumeFromKafka);
		consumerProperties.put("group.id", request.consumerGroupId);
		consumerProperties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		consumerProperties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		if (request.groupInstanceId.length() > 0) {
			consumerProperties.put("group.instance.id", request.groupInstanceId);
		}
		consumerProperties.put("client.id", "kafkaRun-partition-transfer-consumer-" + System.currentTimeMillis());

		String hostname = InetAddress.getLocalHost().getHostName();
		consumerProperties.put("client.rack", hostname);

		producerProperties.put("bootstrap.servers", request.produceToKafka);
		producerProperties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("client.id", "kafkaRun-partition-transfer-producer-" + System.currentTimeMillis());

		Consumer<String, Object> consumer = new KafkaConsumer<>(consumerProperties);
		KafkaProducer<String, Object> producer = new KafkaProducer<>(producerProperties);

		TopicPartition topicPartition = new TopicPartition(request.consumeFromTopic, request.consumeFromPartition);
		consumer.assign(Collections.singletonList(topicPartition));
		consumer.seek(topicPartition, request.consumeFromOffset);

		while (request.epoch > 0) {
			ConsumerRecords<String, Object> crs = consumer.poll(Duration.ofMillis(request.pollTime));
			for (ConsumerRecord<String, Object> record : crs) {
				System.out.println("**************************** Partition Message Transferred ****************************");
				System.out.printf("Consumed from Topic: %s%n", record.topic());
				System.out.printf("Consumed from Partition: %s%n", record.partition());
				System.out.printf("Consumed Offset: %s%n", record.offset());
				System.out.printf("Consumed Value: %s%n", record.value());
				System.out.println("----------------------");
				ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
						request.produceToTopic,
						request.produceToPartition,
						record.key(),
						record.value()
				);

				producer.send(producerRecord);
				System.out.printf("Produced To Topic: %s%n", producerRecord.topic());
				System.out.printf("Produced To Partition: %s%n", producerRecord.partition());
				System.out.println("***************************************************************************************");
			}
			request.epoch -= 1;
		}

		consumer.close();
		producer.close();

		JsonObject response = new JsonObject();
		response.addProperty("consumer-client.id", consumerProperties.getProperty("client.id"));
		response.addProperty("producer-client.id", producerProperties.getProperty("client.id"));
		return gson.toJson(response);
	}
}
