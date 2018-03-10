package client

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestGetAllTopicInfo(t *testing.T) {
	config := sarama.NewConfig()
	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicInfo, err := TopicsInfo(client)
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range topicInfo {
		t.Log(topic.Name)
	}

	topicInfo, err = TopicsInfo(client, "dialogbox", "_schemas")
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range topicInfo {
		t.Log(topic.Name)
	}
}

func TestBrokerMetadata(t *testing.T) {
	broker := sarama.NewBroker("localhost:9092")
	err := broker.Open(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()

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
