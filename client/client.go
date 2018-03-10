package client

import (
	"fmt"

	"github.com/Shopify/sarama"
)

func TopicsInfo(client sarama.Client, topics ...string) ([]*sarama.TopicMetadata, error) {
	var err error
	if len(topics) == 0 {
		topics, err = client.Topics()
		if err != nil {
			return nil, err
		}
	}

	brokers := client.Brokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("No broker is available")
	}
	broker := brokers[0]
	err = broker.Open(nil)
	if err != nil {
		return nil, err
	}
	defer broker.Close()

	metas, err := broker.GetMetadata(&sarama.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return nil, err
	}

	return metas.Topics, nil
}
