package com.myntra.kafkarun.fixes;

import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class CommitService {

	private static final AtomicBoolean shouldStop = new AtomicBoolean();
	Thread thread;

	public String transferPartition() {
		shouldStop.set(false);
		thread = new Thread(new ManualCommitConsumer(shouldStop));
		thread.start();
		return null;
	}

	public String stopTransferPartition() {
		shouldStop.set(true);
		return null;
	}
}
