package client

import (
	"strings"

	cluster "github.com/bsm/sarama-cluster"
)

type KafkaClient struct {
	brokers []string
	config  *cluster.Config
}

func NewKafkaClient(brokerlist string) *KafkaClient {
	brokers := strings.Split(brokerlist, ",")
	if len(brokers) == 0 {
		return nil
	}

	return &KafkaClient{
		brokers: brokers,
		config:  nil,
	}
}
