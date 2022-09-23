package com.myntra.kafkarun.service;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
@Service
public class PartitionTransferService {
	@Autowired
	Gson gson;

	@SneakyThrows
	public String transferPartition(PartitionTransferRequest request) {
		Properties consumerProperties = new Properties();
		Map<String, Object> producerProperties = new HashMap<>();

		consumerProperties.put("bootstrap.servers", request.consumeFromKafka);
		consumerProperties.put("group.id", request.consumerGroupId);
		consumerProperties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		consumerProperties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		consumerProperties.put("enable.auto.commit", false);
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
		ProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties);
		KafkaTemplate<String, Object> producer = new KafkaTemplate<>(producerFactory);

		Consumer<String, Object> consumer = new KafkaConsumer<>(consumerProperties);
		TopicPartition topicPartition = new TopicPartition(request.consumeFromTopic, request.consumeFromPartition);
		consumer.assign(Collections.singletonList(topicPartition));
		consumer.seek(topicPartition, request.consumeFromOffset);

		final long[] messageCount = {0L};
		log.info("================= Starting Partition Transfer =================");
		while (request.epoch > 0) {
			ConsumerRecords<String, Object> crs = consumer.poll(Duration.ofMillis(request.pollTime));
			for (ConsumerRecord<String, Object> record : crs) {
				log.debug("**************************** Partition Message Transferred ****************************");
				log.debug("Consumed from Topic: {}", record.topic());
				log.debug("Consumed from Partition: {}", record.partition());
				log.debug("Consumed Offset: {}", record.offset());
				log.debug("Consumed Value: {}", record.value());
				log.debug("----------------------");
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
						log.error("Failed to push message: " +  producerRecord);
					}

					@Override
					public void onSuccess(SendResult<String, Object> result) {
						log.debug(result.toString());
						log.debug("Produced To Topic: {}", producerRecord.topic());
						log.debug("Produced To Partition: {}", producerRecord.partition());
						messageCount[0] += 1L;
					}
				});

				TopicPartition partition = new TopicPartition(record.topic(), record.partition());
				OffsetAndMetadata offset = new OffsetAndMetadata(record.offset());
				Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
				commitData.put(partition, offset);
				consumer.commitSync(commitData);
			}
			log.info("Transferred {} messages till now", messageCount[0]);
			request.epoch -= 1;
		}

		consumer.close();
		log.info("================= Partition Transfer Ends =================");
		JsonObject response = new JsonObject();
		response.addProperty("consumer-client.id", consumerProperties.getProperty("client.id"));
		response.addProperty("producer-client.id", (String) producerProperties.get("client.id"));
		return gson.toJson(response);
	}
}
