curl --request POST \
  --url http://localhost:8080/consume-produce/start \
  --header 'Content-Type: application/json' \
  --data '{
	"consumerBootstrap": "localhost:9092",
	"producerBootstrap": "localhost:9092",

	"consumeFromTopic": "countspread",
	"consumerGroupId": "kafkarun-consumeproduce-countspread-consumer-group",

	"consumeFromPartitionStartOffset": {
		"0" : "0",
		"1" : "0",
		"2" : "0",
		"3" : "0",
		"4" : "0"
	},

	"consumeFromPartitionEndOffset": {
		"0" : "16",
		"1" : "3",
		"2" : "2",
		"3" : "0",
		"4" : "4"
	},

	"produceToTopic": "counter",
	"consumeToProducePartition": {
		"0": "2",
		"1": "2",
		"2": "2",
		"3": "2",
		"4": "2"
	}
}'
