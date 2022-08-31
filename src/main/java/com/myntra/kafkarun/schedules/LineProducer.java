package com.myntra.kafkarun.schedules;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.stream.Stream;

@Component
public class LineProducer {

	@SneakyThrows
	@Scheduled(initialDelay = 10000, fixedDelay=Long.MAX_VALUE)
	public void produceFileToTopic() {
		System.out.println("Transfer messages in File to Topic; Started at " + new Date());

		String fileName = "/tmp/messages.txt";
		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
			stream.forEach(this::processLine);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@SneakyThrows
	public void processLine(String line) {
		kafkaTemplate.send("dc1.Pretr-PUSH_UPDATE_EVENT_TO_SELLER", null, line);
		System.out.println("################# Produced message #################");
		System.out.println(line);
		System.out.println("#####################################################");
	}

}
