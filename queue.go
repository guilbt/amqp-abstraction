package amqp_abstraction

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

const RetriesHeader = "x-retries"

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

func CreateDLQ(ch *amqp.Channel, queueName string) (dlxName string, dlqName string, err error) {
	dlxName = fmt.Sprintf("%s-dlx", queueName)
	dlqName = fmt.Sprintf("%s-dlq", queueName)
	if err := ch.ExchangeDeclare(
		dlxName, "fanout", true, false, false, false, nil,
	); err != nil {
		return dlxName, dlqName, err
	}
	if err := CreateSimpleQueue(ch, dlqName, nil); err != nil {
		return dlxName, dlqName, err
	}
	err = ch.QueueBind(
		dlqName, "", dlxName, false, nil,
	)
	return dlxName, dlqName, err
}

func CreateQueueWithDLQ(ch *amqp.Channel, properties *QueueProperties) error {
	if dlxName, dlqName, err := CreateDLQ(ch, properties.QueueName); err != nil {
		return err
	} else {
		err = CreateSimpleQueue(
			ch,
			properties.QueueName,
			amqp.Table{
				"x-dead-letter-exchange": dlxName,
				"x-dead-letter-routing-key": dlqName,
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
