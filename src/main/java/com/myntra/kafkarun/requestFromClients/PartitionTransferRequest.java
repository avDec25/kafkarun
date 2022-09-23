package com.myntra.kafkarun.requestFromClients;

public class PartitionTransferRequest {
	public String consumeFromKafka;
	public String produceToKafka;

	public String consumeFromTopic;
	public String produceToTopic;
	public int consumeFromPartition;
	public int produceToPartition;
	public String consumerGroupId;
	public Long consumeFromOffset;

	public long pollTime;
	public int epoch;
	public String groupInstanceId = null;
}
