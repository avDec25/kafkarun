package com.myntra.kafkarun.controller;

import com.myntra.kafkarun.requestFromClients.PartitionTransferRequest;
import com.myntra.kafkarun.service.PartitionTransferService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("partition")
public class PartitionController {

	@Autowired
	PartitionTransferService partitionTransferService;

	@PostMapping("transfer")
	public ResponseEntity<?> consumeFromPartitionToProduceInPartition(@RequestBody PartitionTransferRequest request) {
		return ResponseEntity.ok().body(partitionTransferService.transferPartition(request));
	}

//	public ResponseEntity<?> fileToPartition(@RequestBody FileToPartitionTransferRequest request) {
//		return ResponseEntity.ok().body(partitionTransferService.transferFromFileToPartition(request));
//	}

}
