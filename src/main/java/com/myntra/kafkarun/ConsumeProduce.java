package com.myntra.kafkarun;

import com.myntra.kafkarun.requestFromClients.ConsumeProduceOffsetRequest;
import com.myntra.kafkarun.requestFromClients.SimpleConsumeProduceRequest;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.net.InetAddress;
import java.time.Duration;
import java.util.*;

@RequestMapping("consume-produce")
@Controller
public class ConsumeProduce {
	Properties consumerProperties = new Properties();
	Properties producerProperties = new Properties();
	Consumer<String, Object> consumer = null;
	Collection<String> allTopics = new HashSet<>();
	KafkaProducer<String, Object> produceTo = null;
	boolean run = true;
	boolean simpleRun = true;

	@SneakyThrows
	@PostMapping("simple/start")
	public ResponseEntity<?> simpleConsumeProduce(@RequestBody SimpleConsumeProduceRequest request) {
		consumerProperties.put("bootstrap.servers", request.consumerBootstrap);
		consumerProperties.put("group.id", request.consumerGroupId);
		consumerProperties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		consumerProperties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
//		consumerProperties.put("group.instance.id", "1");
		consumerProperties.put("client.id", "kafka-run-cp-consumer-" + System.currentTimeMillis());

		String hostname = InetAddress.getLocalHost().getHostName();
		consumerProperties.put("client.rack", hostname);

		producerProperties.put("bootstrap.servers", request.producerBootstrap);
		producerProperties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
		producerProperties.put("client.id", "kafka-run-cp-producer-" + System.currentTimeMillis());
		produceTo = new KafkaProducer<String, Object>(producerProperties);

		consumer = new KafkaConsumer<>(consumerProperties);
		consumer.subscribe(Collections.singletonList(request.consumeFromTopic));

		simpleRun = true;
		while (simpleRun) {
			ConsumerRecords<String, Object> crs = consumer.poll(Duration.ofMillis(100L));
			for (ConsumerRecord<String, Object> record : crs) {
				System.out.println("**************************** Consumed Record ****************************");
				System.out.printf("Consumed from Topic: %s%n", record.topic());
				System.out.printf("Consumed from Partition: %s%n", record.partition());
				System.out.printf("Consumed Offset: %s%n", record.offset());
				System.out.printf("Consumed Value: %s%n", record.value());
				System.out.println("*************************************************************************");
				ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
						request.produceToTopic,
						record.key(),
						record.value()
				);
				produceTo.send(producerRecord);
				System.out.printf("Produced To Topic: %s%n", request.produceToTopic);
				System.out.println("**************************** Produced Record ****************************");
				if (!simpleRun) {
					break;
				}
			}
		}
		return ResponseEntity.ok().body("Done Simple Consume and Produce");
	}


	@SneakyThrows
	@PostMapping("start")
	public ResponseEntity<?> consumerFromProduceTo(@RequestBody ConsumeProduceOffsetRequest request) {
		try {
			consumerProperties.put("group.id", request.consumerGroupId);
			consumerProperties.put("bootstrap.servers", request.consumerBootstrap);
			consumerProperties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
			consumerProperties.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
			consumerProperties.put("group.instance.id", "1");
			consumerProperties.put("client.id", "kafka-run-cp-consumer-" + System.currentTimeMillis());

			String hostname = InetAddress.getLocalHost().getHostName();
			consumerProperties.put("client.rack", hostname);


			producerProperties.put("bootstrap.servers", request.producerBootstrap);
			producerProperties.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
			producerProperties.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
			producerProperties.put("client.id", "kafka-run-cp-producer-" + System.currentTimeMillis());
			produceTo = new KafkaProducer<String, Object>(producerProperties);


			for (Map.Entry<Integer, Integer> consumeFromProduceToPartition : request.consumeToProducePartition.entrySet()) {
				consumer = new KafkaConsumer<>(consumerProperties);
				Integer consumeFromPartition = consumeFromProduceToPartition.getKey();
				Integer produceToPartition = consumeFromProduceToPartition.getValue();

				Long consumeStartOffset = request.consumeFromPartitionStartOffset.get(consumeFromPartition);
				Long consumeEndOffset = request.consumeFromPartitionEndOffset.get(consumeFromPartition);

				System.out.println("................... Transfer Started ...................");
				System.out.println("Consuming from Partition: " + consumeFromPartition);
				System.out.println("Consume Start offset: " + consumeStartOffset);
				System.out.println("Consume End offset: " + consumeEndOffset);
				System.out.println("Produce into partition: " + produceToPartition);
				System.out.println("........................................................");

				TopicPartition tp = new TopicPartition(request.consumeFromTopic, consumeFromPartition);
				consumer.subscribe(Collections.singletonList(request.consumeFromTopic), new ConsumerRebalanceListener() {
					@Override
					public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					}

					@Override
					public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
						// moving to desired offset
						consumer.seek(tp, consumeStartOffset);
					}
				});

				run = true;
				while (run) {
					ConsumerRecords<String, Object> crs = consumer.poll(Duration.ofMillis(100L));
					for (ConsumerRecord<String, Object> record : crs) {
						System.out.println("**************************** Consumed Record ****************************");
						System.out.printf("Consumed from Topic: %s%n", record.topic());
						System.out.printf("Consumed from Partition: %s%n", record.partition());
						System.out.printf("Consumed Offset: %s%n", record.offset());
						System.out.printf("Consumed Value: %s%n", record.value());
						System.out.println("*************************************************************************");
						ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
								request.produceToTopic,
								produceToPartition,
								record.key(),
								record.value()
						);
						produceTo.send(producerRecord);
						System.out.printf("Produced To Topic: %s%n", request.produceToTopic);
						System.out.printf("Produced To Partition: %d%n", produceToPartition);
						System.out.println("**************************** Produced Record ****************************");
						if (record.offset() == consumeEndOffset) {
							// Reached the end offset, stop consuming
							run = false;
							break;
						}
					}
				}
				consumer.close();
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			consumer.close();
		}
		return ResponseEntity.ok().body("Done.");
	}

	@PostMapping("stop")
	public ResponseEntity<?> stopConsumeProduce() {
		try {
			run = false;
			simpleRun = false;
			consumer.close();
			produceTo.close();
			return ResponseEntity.ok("Consumer and Producer are Closed now");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ResponseEntity.ok("Shutdown Failed");
	}

}
