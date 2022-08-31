package com.myntra.kafkarun;

import java.util.Map;

public class ConsumeProduceOffsetRequest {
	public String consumerBootstrap;
	public String producerBootstrap;

	public String consumeFromTopic;
	public String consumerGroupId;
	public Map<Integer, Long> consumeFromPartitionStartOffset;
	public Map<Integer, Long> consumeFromPartitionEndOffset;

	public String produceToTopic;
	public Map<Integer, Integer> consumeToProducePartition;
}
