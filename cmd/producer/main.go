package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"fmt"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferenci", "teste", producer, []byte("transferencia"), deliveryChan)
	go DeliveryReport(deliveryChan) // async - run the delivery report on another thread

	producer.Flush(5000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":    "kafka-101_kafka_1:9092",
		"delivery.timeout.ms" : "0", // 0 = no timeout
		"acks":                 "all", // 0 (doesn't return a response) / 1 (Leader confirms the delivery) / all (Leader and followers confirm the delivery)
		"enable.idempotence":   "true", // if true, acks must be "all"
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Failed to send message")
			} else {
				fmt.Println("Message sent successfully:", ev.TopicPartition)
			}
		}
	}
}