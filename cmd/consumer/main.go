package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"fmt"
)

func main() {

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-101_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest", // reads all the messages from all partitions since the beginning
	}
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		fmt.Println("error consumer", err.Error())
	}

	topics := []string{"teste"}
	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1) // -1 = keep connected
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}