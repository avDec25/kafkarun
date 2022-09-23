package com.myntra.kafkarun.requestFromClients;

public class PartitionTransferRequest {
	public String consumeFromKafka;
	public String produceToKafka;

	public String consumeFromTopic;
	public Long consumeFromOffset;
	public String consumerGroupId;
	public String produceToTopic;

	public int consumeFromPartition;
	public int produceToPartition;
	public long pollTime;
	public int epoch;
	public String groupInstanceId = null;
}
