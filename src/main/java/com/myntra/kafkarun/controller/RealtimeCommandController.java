package com.myntra.kafkarun.controller;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.myntra.kafkarun.requestFromClients.ConsumerGroupDetailsRequest;
import com.myntra.kafkarun.utils.StreamGobbler;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
@Controller
@RequestMapping("realtime")
public class RealtimeCommandController {
	@Value("${confluent.home}")
	String CONFLUENT_HOME;

	public static final String BROADCAST_TOPIC = "/topic/consumer-group-describe";
	Gson gson = new GsonBuilder().disableHtmlEscaping().create();

	@Autowired
	SimpMessagingTemplate messagingTemplate;

	@SneakyThrows
	@PostMapping("consumer-group/describe")
//	@MessageMapping("consumer-group/describe")
	public ResponseEntity<?> describeConsumerGroup(@RequestBody ConsumerGroupDetailsRequest request) {
		String cmd = String.format(
				"%s/bin/%s --bootstrap-server %s --describe --group %s",
				CONFLUENT_HOME,
				"kafka-consumer-groups",
				request.bootstrapServers,
				request.consumerGroupId
		);
		ProcessBuilder builder = new ProcessBuilder();
		builder.command(cmd.split(" "));
		builder.directory(new File(System.getProperty("user.home")));
		Process process = builder.start();
		StreamGobbler streamGobbler = new StreamGobbler(process.getInputStream(), this::emit);
		Future<?> future = Executors.newSingleThreadExecutor().submit(streamGobbler);
		int exitCode = process.waitFor();
		assert exitCode == 0;
		future.get(10, TimeUnit.SECONDS);
		return ResponseEntity.ok().build();
	}

	private void emit(String message) {
		JsonObject response = new JsonObject();
		response.addProperty("timestamp", "" + new Date());
		response.addProperty("message", message);
		messagingTemplate.convertAndSend(BROADCAST_TOPIC, gson.toJson(response));
		log.info(message);
	}
}
