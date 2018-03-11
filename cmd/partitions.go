// Copyright © 2018 Jason Kim <dialogbox@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"fmt"
	"sort"

	"github.com/dialogbox/gofka/kafka"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// partitionsCmd represents the partitions command
var partitionsCmd = &cobra.Command{
	Use:   "partitions",
	Short: "Print list of partitions",
	Run: func(cmd *cobra.Command, args []string) {
		printPartitions(args)
	},
}

func init() {
	rootCmd.AddCommand(partitionsCmd)
}

func printPartitions(topics []string) {
	client, err := kafka.NewClient("localhost:9092")
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	topicInfos, err := client.TopicInfos(topics...)
	if err != nil {
		logrus.Fatal(err)
	}

	for _, topicInfo := range topicInfos {
		fmt.Println("Topic : ", topicInfo.Name)

		parts := topicInfo.Partitions
		sort.Slice(parts, func(i, j int) bool {
			return parts[i].ID < parts[j].ID
		})

		for _, part := range parts {
			fmt.Printf("ID: %v\tLeader: %v\tReplica: %v\tIsr: %v\n", part.ID, part.Leader, part.Replicas, part.Isr)
		}
	}
}
