package consumer

import (
	"go-rabbitmq/config"
	"log"
)

func ReceiveMessageFromQueue() {
	queue := config.GetQueue()
	queue.Consume(func(msg string) {
		log.Printf("Received message: %s", msg)
	})
}
