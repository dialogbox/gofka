package kafka

import (
	"testing"
	"time"

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

	topicInfo, err = client.TopicInfos("__consumer_offsets", "_schemas")
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
		Topics: []string{"__consumer_offsets", "_schemas"},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, meta := range metas.Topics {
		t.Logf("%s, %v", meta.Name, meta.Partitions)
	}
}

func TestOffsetRange(t *testing.T) {
	testTopic := "testtopic"
	client, err := NewClient("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topicInfo, err := client.TopicInfos(testTopic)
	if err != nil {
		t.Fatal(err)
	}

	for _, partInfo := range topicInfo[0].Partitions {
		lw, hw, err := client.OffsetRange(testTopic, partInfo.ID)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("%s[%v] : %v ~ %v\n", testTopic, partInfo.ID, lw, hw)
	}
}

func TestListGroups(t *testing.T) {
	// testTopic := "testtopic"
	client, err := NewClient("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	groups, err := client.ListGroups()
	if err != nil {
		t.Fatal(err)
	}

	t.Log(groups)
}

func TestFetchData(t *testing.T) {
	testTopic := "testtopic"
	client, err := NewClient("localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	lw, hw, err := client.OffsetRange(testTopic, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v ~ %v", lw, hw)

	messages, err := client.FetchData(testTopic, 0, lw, 10, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	for _, m := range messages {
		t.Logf("%v, %v, Header: %v, Key: [%v], Value: [%v]", m.Offset, m.Timestamp, m.Headers, m.Key, string(m.Value))
	}
}
