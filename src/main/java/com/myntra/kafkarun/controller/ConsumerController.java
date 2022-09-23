package com.myntra.kafkarun.controller;

import com.myntra.kafkarun.config.NormalConsumer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@Slf4j
@Controller
@RequestMapping("consume")
public class ConsumerController {

	@Autowired
	NormalConsumer normalConsumer;

	@SneakyThrows
	@PostMapping("data/{topic}")
	public ResponseEntity<?> consumeDataFromTopic(@PathVariable("topic") String topicName) {
		normalConsumer.consume(topicName);
		return ResponseEntity.ok("consumed");
	}

}
