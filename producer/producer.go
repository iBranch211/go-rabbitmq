package producer

import (
	"go-rabbitmq/config"
	"log"
)

func SendMessageToQueue(message string) {
	log.Println("Sending message...")
	queue := config.GetQueue()
	queue.Send(message)
}
