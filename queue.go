package amqp_abstraction

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type QueueProperties struct {
	ExchangeName string
	QueueName    string
}

func CreateSimpleQueue(ch *amqp.Channel, queueName string, args amqp.Table) error {
	_, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args,
	)
	return err
}

func CreateDLQ(ch *amqp.Channel, queueName string) (dlxName string, err error) {
	dlxName = fmt.Sprintf("%s-dlx", queueName)
	dlqName := fmt.Sprintf("%s-dlq", queueName)
	if err := ch.ExchangeDeclare(
		dlxName, "fanout", true, false, false, false, nil,
	); err != nil {
		return dlxName, err
	}
	if err := CreateSimpleQueue(ch, dlqName, nil); err != nil {
		return dlxName, err
	}
	err = ch.QueueBind(
		dlqName, "", dlxName, false, nil,
	)
	return dlxName, err
}

func CreateQueueWithDLQ(ch *amqp.Channel, properties *QueueProperties) error {
	if dlxName, err := CreateDLQ(ch, properties.QueueName); err != nil {
		return err
	} else {
		err = CreateSimpleQueue(
			ch,
			properties.QueueName,
			amqp.Table{
				"x-dead-letter-exchange": dlxName,
			},
		)
		return err
	}
}

type QueueEvent struct {
	QueueProperties *QueueProperties
	Body            interface{}
}

func SendMessage(ch *amqp.Channel, queueEvent QueueEvent) error {
	payload, err := json.Marshal(queueEvent.Body)
	if err != nil {
		return err
	}
	err = ch.Publish(
		queueEvent.QueueProperties.ExchangeName, // exchange
		queueEvent.QueueProperties.QueueName,    // routing key
		false,                                   // mandatory
		false,                                   // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         payload,
			Timestamp:    time.Now(),
			DeliveryMode: amqp.Persistent,
		},
	)

	return err
}
