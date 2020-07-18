package main

import (
	"go-rabbitmq/config"
	"go-rabbitmq/consumer"
	"go-rabbitmq/producer"
)

func main() {
	config.NewQueue()
	defer config.GetQueue().Close()

	producer.SendMessageToQueue("Hello World!")
	consumer.ReceiveMessageFromQueue()

}
