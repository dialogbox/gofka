package client

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestGetTopicInfo(t *testing.T) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		t.Fatal(err)
	}

	topicInfo, err := AllTopicInfo(client)
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range topicInfo {
		t.Log(topic)
	}
}

func TestBrokerMetadata(t *testing.T) {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		t.Fatal(err)
	}

	metas, err := broker.GetMetadata(&sarama.MetadataRequest{
		Topics: []string{"dialogbox", "_schemas"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, meta := range metas.Topics {
		t.Logf("%s, %v", meta.Name, meta.Partitions)
	}
}
