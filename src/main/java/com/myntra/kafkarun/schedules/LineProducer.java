package com.myntra.kafkarun.schedules;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

@Component
public class LineProducer {

	@Value("${line-producer.source-file.path}")
	String SOURCE_FILE_PATH;

	@Value("${line-producer.destination-topic.name}")
	String DESTINATION_TOPIC_NAME;

	@Value("${line-producer.produce-delay}")
	Integer PRODUCE_DELAY;

	@SneakyThrows
	@Scheduled(initialDelay = 10000, fixedDelay=Long.MAX_VALUE)
	public void produceFileToTopic() {
		System.out.println("Transfer messages in File to Topic; Started at " + new Date());

		try (Stream<String> stream = Files.lines(Paths.get(SOURCE_FILE_PATH))) {
			stream.forEach(this::processLine);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@SneakyThrows
	public void processLine(String line) {
		kafkaTemplate.send(DESTINATION_TOPIC_NAME, null, line);
		System.out.println("################# Produced message #################");
		System.out.println(line);
		System.out.println("#####################################################");
		Thread.sleep(PRODUCE_DELAY);
	}

}
