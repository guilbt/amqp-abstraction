package examples

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

func RabbitMQConnect() (ch *amqp.Channel, err error) {
	rabbitUrl := "amqp://rabbitmq:rabbitmq@localhost:5672"
	var conn *amqp.Connection
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(rabbitUrl)
		if err != nil {
			log.Println("failed to connect to RabbitMQ, retrying in 2 Seconds...")
			time.Sleep(2 * time.Second)
		}
	}
	if err != nil {
		return nil, err
	}

	if ch, err := conn.Channel(); err != nil {
		return nil, err
	} else {
		log.Println("Connected to RabbitMQ")
		return ch, err
	}
}

func ExitApplicationOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
