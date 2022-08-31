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
//	@Scheduled(initialDelay = 10000, fixedDelay=Long.MAX_VALUE)
	public void produceFileToTopic() {
		System.out.println("Starting Slow Message Production " + new Date());

		String fileName = "/tmp/messagesData.txt";
		try (Stream<String> stream = Files.lines(Paths.get(fileName))) {
			stream.forEach(this::processLine);
		}
	}

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@SneakyThrows
	public void processLine(String line) {
		Thread.sleep(1000);
		System.out.println("################# Producing message #################");
		System.out.println(line);
		System.out.println("#####################################################");
		int partition = 5;
		kafkaTemplate.send("dc1.Pretr-PUSH_UPDATE_EVENT_TO_SELLER", partition, null, line);
	}

}
