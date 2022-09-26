package com.myntra.kafkarun.controller;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.myntra.kafkarun.functionality.PartitionTransferrer;
import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.concurrent.atomic.AtomicBoolean;

@Controller
@RequestMapping("partition")
public class PartitionController {
	@Autowired
	Gson gson;

	@Autowired
	SimpMessagingTemplate messagingTemplate;

	private static final AtomicBoolean shouldStop = new AtomicBoolean();
	Thread thread;

	@PostMapping("transfer/start")
	public ResponseEntity<String> startPartitionTransfer(@RequestBody PartitionTransferRequest request) {
		shouldStop.set(false);
		thread = new Thread(new PartitionTransferrer(shouldStop, request, messagingTemplate));
		thread.start();

		JsonObject response = new JsonObject();
		response.addProperty("message", "Started Partition Transfer with Process Id: " + thread.getId());
		return ResponseEntity.ok().body(gson.toJson(response));
	}

	@SneakyThrows
	@PostMapping("transfer/stop")
	public ResponseEntity<String> stopPartitionTransfer() {
		shouldStop.set(true);
		thread.join();
		JsonObject response = new JsonObject();
		response.addProperty("message", "Stopped Partition Transfer Process of Id: " + thread.getId());
		return ResponseEntity.ok().body(gson.toJson(response));
	}

}
