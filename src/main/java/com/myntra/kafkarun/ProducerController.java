package com.myntra.kafkarun;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Controller;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.Date;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequestMapping("produce")
@Controller
public class ProducerController {
	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@SneakyThrows
	@PostMapping("data/{topic}")
	public ResponseEntity<?> produceDataInTopic(@PathVariable("topic") String topicName, @RequestBody String message) {
		Gson gson = new GsonBuilder().create();
		JsonObject data = new JsonObject();
		data.addProperty("timestamp", new Date().toString());
		data.addProperty("message", message);

		ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topicName, gson.toJson(data));
		future.addCallback(new ListenableFutureCallback<>() {
			@Override
			public void onFailure(@NonNull Throwable ex) {
				ex.printStackTrace();
				System.out.println("failed");
			}

			@Override
			public void onSuccess(SendResult<String, Object> result) {
				System.out.println(result.toString());
				log.info("successfully produced message");
			}
		});

		SendResult<String, Object> sendResult = future.get(10, TimeUnit.SECONDS);
		return ResponseEntity.ok(sendResult.toString());
	}

}
