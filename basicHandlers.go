package amqp_abstraction

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

func fetchConnectTryingThrice(amqpUrl string) (conn *amqp.Connection, err error) {
	for i := 0; i < 3; i++ {
		conn, err = amqp.Dial(amqpUrl)
		if err != nil {
			log.Println("failed to connect to AMQP Service, retrying in 2 Seconds...")
			time.Sleep(2 * time.Second)
		} else {
			return conn, err
		}
	}
	return nil, err
}


func RabbitMQConnect() (ch *amqp.Channel, err error) {
	rabbitUrl := "amqp://rabbitmq:rabbitmq@localhost:5672"
	conn, err := fetchConnectTryingThrice(rabbitUrl)
	if err != nil {
		return nil, err
	}
	if ch, err := conn.Channel(); err != nil {
		return nil, err
	} else {
		log.Println("Connected to AMQP Service")
		return ch, err
	}
}

func ExitApplicationOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
