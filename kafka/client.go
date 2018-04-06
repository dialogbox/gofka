package kafka

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

// Client is simplified Kafka client to manage and inspect Kafka topic
type Client struct {
	bootstrapServers []string
	client           sarama.Client
}

// NewClient connects to the specified brokers.
// You should close the client after using it.
func NewClient(bootstrapServers ...string) (*Client, error) {
	if len(bootstrapServers) == 0 {
		return nil, fmt.Errorf("Atleast one bootstrap server must be provided")
	}

	config := sarama.NewConfig()
	config.Version = sarama.V1_0_0_0
	client, err := sarama.NewClient(bootstrapServers, config)
	if err != nil {
		return nil, err
	}

	return &Client{
		bootstrapServers: bootstrapServers,
		client:           client,
	}, nil
}

// Close close and cleanup the client
func (g *Client) Close() error {
	return g.client.Close()
}

// TopicNames returns list of topic from the connected cluster
func (g *Client) TopicNames() ([]string, error) {
	topics, err := g.client.Topics()
	if err != nil {
		return nil, err
	}

	return topics, nil
}

// TopicInfos returns status and other useful informations of requested topics.
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
	defer func() {
		if err := broker.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	metas, err := broker.GetMetadata(&sarama.MetadataRequest{
		Topics: topics,
	})
	if err != nil {
		return nil, err
	}

	return metas.Topics, nil
}

// OffsetRange returns min/max offset of the single partition
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

func (g *Client) ListGroups() ([]*sarama.GroupDescription, error) {
	brokers := g.client.Brokers()
	if len(brokers) == 0 {
		return nil, fmt.Errorf("No broker is available")
	}

	b := brokers[0]
	b.Open(nil)
	defer func() {
		if err := b.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	res, err := b.ListGroups(nil)
	if err != nil {
		return nil, err
	}

	for k, v := range res.Groups {
		log.Printf("%v, %v", k, v)
	}

	return nil, nil
}

// FetchData fetches messages within specific offset range of single partition
// The main purpose of this function is to access to any single topic partition's data in random access manner.
func (g *Client) FetchData(topic string, part int32, offset int64, n int32, timeout time.Duration) ([]*sarama.ConsumerMessage, error) {
	offsetMgr, err := sarama.NewOffsetManagerFromClient("gofka-client", g.client)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := offsetMgr.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partOffsetMgr, err := offsetMgr.ManagePartition(topic, part)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := partOffsetMgr.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partOffsetMgr.MarkOffset(offset, "Random Access by gofka")
	log.Println(partOffsetMgr.NextOffset())
	partOffsetMgr.ResetOffset(offset, "Random Access by gofka")
	log.Println(partOffsetMgr.NextOffset())

	consumer, err := sarama.NewConsumerFromClient(g.client)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	partConsumer, err := consumer.ConsumePartition(topic, part, offset)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := partConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

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
