package com.myntra.kafkarun.listeners;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

@Component
public class SpawnedConsumers {


	long num = 100;

//	@SneakyThrows
//	@KafkaListener(topics = "amardelta", groupId = "amar-consumers-group")
//	@KafkaListener(
//			topicPartitions = @TopicPartition(
//					topic = "amardelta",
//					partitions = {"0", "1"}
//			)
//	)
//	public void spawn(ConsumerRecord<String, Object> record) {
//		Thread.sleep(num);
//		System.out.printf("Consumer 1; Partition: %d, Offset: %d\n", record.partition(), record.offset());
//	}
//
//	@SneakyThrows
//	@KafkaListener(topics = "dolphin", groupId = "ocean-consumer-group")
//	public void spawn2(ConsumerRecord<String, Object> record) {
//		Thread.sleep(num);
//		System.out.printf("Consumer 2; Partition: %d, Offset: %d\n", record.partition(), record.offset());
//	}
//
//	@SneakyThrows
//	@KafkaListener(topics = "amardelta", groupId = "amar-consumers-group")
//	public void spawn3(ConsumerRecord<String, Object> record) {
//		Thread.sleep(num);
//		System.out.printf("Consumer 3; Partition: %d, Offset: %d\n", record.partition(), record.offset());
//	}
//
//	@SneakyThrows
//	@KafkaListener(topics = "amardelta", groupId = "amar-consumers-group")
//	public void spawn4(ConsumerRecord<String, Object> record) {
//		Thread.sleep(num);
//		System.out.printf("Consumer 4; Partition: %d, Offset: %d\n", record.partition(), record.offset());
//	}
//
//	@SneakyThrows
//	@KafkaListener(topics = "amardelta", groupId = "amar-consumers-group")
//	public void spawn5(ConsumerRecord<String, Object> record) {
//		Thread.sleep(num);
//		System.out.printf("Consumer 5; Partition: %d, Offset: %d\n", record.partition(), record.offset());
//	}


}
