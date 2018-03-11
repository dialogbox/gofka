package kafka

import (
	"testing"

	"github.com/Shopify/sarama"
)

func TestGetTopicInfos(t *testing.T) {
	client, err := NewClient("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicInfo, err := client.TopicInfos()
	if err != nil {
		t.Fatal(err)
	}

	for _, topic := range topicInfo {
		t.Log(topic.Name)
	}

	topicInfo, err = client.TopicInfos("dialogbox", "_schemas")
	if err != nil {
		t.Fatal(err)
	}

	for i := range topicInfo {
		t.Log(topicInfo[i].Name)
	}
}

func TestGetTopicNames(t *testing.T) {
	client, err := NewClient("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicNames, err := client.TopicNames()
	if err != nil {
		t.Fatal(err)
	}

	for i := range topicNames {
		t.Log(topicNames[i])
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
