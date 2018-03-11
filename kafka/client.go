package kafka

import (
	"fmt"

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
