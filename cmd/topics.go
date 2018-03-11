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

	"github.com/dialogbox/gofka/kafka"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var toggle bool

// topicsCmd represents the topics command
var topicsCmd = &cobra.Command{
	Use:   "topics",
	Short: "Print list of topics",
	Run: func(cmd *cobra.Command, args []string) {
		printTopicList()
	},
}

func init() {
	rootCmd.AddCommand(topicsCmd)
}

func printTopicList() {
	client, err := kafka.NewClient("localhost:9092")
	if err != nil {
		logrus.Fatal(err)
	}
	defer client.Close()

	topics, err := client.TopicNames()
	if err != nil {
		logrus.Fatal(err)
	}

	for i := range topics {
		fmt.Println(topics[i])
	}
}
