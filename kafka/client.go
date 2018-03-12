package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

type Client struct {
	bootstrapServers []string
	client           sarama.Client
}

func NewClient(bootstrapServers ...string) (*Client, error) {
	if len(bootstrapServers) == 0 {
		return nil, fmt.Errorf("Atleast one bootstrap server must be provided")
	}

	config := sarama.NewConfig()
	client, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		bootstrapServers: bootstrapServers,
		client:           client,
	}, nil
}

func (g *Client) Close() error {
	return g.client.Close()
}

func (g *Client) TopicNames() ([]string, error) {
	topics, err := g.client.Topics()
	if err != nil {
		return nil, err
	}

	return topics, nil
}

func (g *Client) TopicInfos(topics ...string) ([]*sarama.TopicMetadata, error) {
	var err error
	if len(topics) == 0 {
		topics, err = g.client.Topics()
		if err != nil {
			return nil, err
		}
	}

	brokers := g.client.Brokers()
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

func (g *Client) OffsetRange(topic string, part int32) (int64, int64, error) {
	lw, err := g.client.GetOffset(topic, part, sarama.OffsetOldest)
	if err != nil {
		return -1, -1, err
	}

	hw, err := g.client.GetOffset(topic, part, sarama.OffsetNewest)
	if err != nil {
		return -1, -1, err
	}

	return lw, hw, nil
}

func (g *Client) FetchData(topic string, part int32, offset int64, n int32, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	consumer, err := sarama.NewConsumerFromClient(g.client)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition(topic, part, offset)
	if err != nil {
		return nil, err
	}

	result := make([]*sarama.ConsumerMessage, 0, n)
	for {
		select {
		case message := <-partConsumer.Messages():
			result = append(result, message)
			if len(result) >= int(n) {
				return result, nil
			}
		case <-time.After(timeout):
			return result, nil
		}
	}
}
