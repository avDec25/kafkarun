package com.myntra.kafkarun.schedules;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

@Slf4j
@Component
public class LineProducer {

	@Value("${line-producer.source-file.path}")
	String SOURCE_FILE_PATH;

	@Value("${line-producer.destination-topic.name}")
	String DESTINATION_TOPIC_NAME;

	@Value("${line-producer.produce-delay}")
	Integer PRODUCE_DELAY;

	@Value("${line-producer.enable}")
	Boolean isEnabled;

	@SneakyThrows
	@Scheduled(initialDelay = 10000, fixedDelay = Long.MAX_VALUE)
	public void produceFileToTopic() {
		if (!isEnabled) {
			return;
		}
		System.out.println("Transfer messages in File to Topic; Started at " + new Date());

		try (Stream<String> stream = Files.lines(Paths.get(SOURCE_FILE_PATH))) {
			stream.forEach(this::processLine);
		} catch (Exception e) {
			log.error("Error: Cannot stream lines from file; Reason: ", e.getCause());
		}
	}

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@SneakyThrows
	public void processLine(String line) {
		kafkaTemplate.send(DESTINATION_TOPIC_NAME, line);
		System.out.println("################# Produced message #################");
		System.out.println(line);
		System.out.println("#####################################################");
		Thread.sleep(PRODUCE_DELAY);
	}

}
